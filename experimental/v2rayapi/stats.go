package v2rayapi

import (
	"context"
	"net"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/bufio"
	E "github.com/sagernet/sing/common/exceptions"
	N "github.com/sagernet/sing/common/network"
  bolt "github.com/sagernet/bbolt" // MOD: 引入 bbolt 用于单文件持久化
)

func init() {
	StatsService_ServiceDesc.ServiceName = "v2ray.core.app.stats.command.StatsService"
}

var (
	_ adapter.ConnectionTracker = (*StatsService)(nil)
	_ StatsServiceServer        = (*StatsService)(nil)
)

type StatsService struct {
	createdAt time.Time
	inbounds  map[string]bool
	outbounds map[string]bool
	users     map[string]bool
	access    sync.Mutex
	counters  map[string]*atomic.Int64
  // MOD: 持久化相关字段
  storeEnabled  bool
  storePath     string
  storeFileID   string
  storeInterval time.Duration
  db            *bolt.DB
  persistLock   sync.Mutex
  lastPersisted map[string]int64 // 上次写入 DB 的值，用于差异写入
  stopCh        chan struct{}
  wg            sync.WaitGroup
}
const boltBucketName = "stats" // MOD: 默认持久化间隔

func NewStatsService(options option.V2RayStatsServiceOptions) *StatsService {
	if !options.Enabled {
		return nil
	}
	inbounds := make(map[string]bool)
	outbounds := make(map[string]bool)
	users := make(map[string]bool)
	for _, inbound := range options.Inbounds {
		inbounds[inbound] = true
	}
	for _, outbound := range options.Outbounds {
		outbounds[outbound] = true
	}
	for _, user := range options.Users {
		users[user] = true
	}
	s := &StatsService{
		createdAt: time.Now(),
		inbounds:  inbounds,
		outbounds: outbounds,
		users:     users,
		counters:  make(map[string]*atomic.Int64),
    // MOD: 添加一个上次的值, 和空结构
    lastPersisted: make(map[string]int64),
    stopCh:        make(chan struct{}),
	}
  // MOD: 读取 store 配置（假定 options.Store 存在并包含 Enabled/Path/V2RayAPIStatsID/Interval）
  // 如果 options.Store 未定义或为零值，则不启用持久化
  if options.Store != nil && options.Store.Enabled {
    s.storeEnabled = true
    if options.Store.Path != "" {
      s.storePath = options.Store.Path
    } else {
      s.storePath = "stats.db"
    }
    s.storeFileID = options.Store.V2RayAPIStatsID
    if options.Store.Interval != 0 {
      s.storeInterval = time.Duration(options.Store.Interval)
    } else {
      s.storeInterval = 30 * time.Second
    }
    // 打开或创建 bbolt 数据库
    db, err := bolt.Open(s.storePath, 0600, &bolt.Options{Timeout: 1 * time.Second})
    if err != nil {
      // 如果打开 DB 失败，则禁用持久化并继续运行（不阻塞主流程）
      s.storeEnabled = false
    } else {
      s.db = db
      // 确保 bucket 存在
      _ = s.db.Update(func(tx *bolt.Tx) error {
        _, err := tx.CreateBucketIfNotExists([]byte(boltBucketName))
        return err
      })
      // 从 DB 恢复已有值到内存计数器（MOD: 反向映射并合并）
      _ = s.loadFromDB()
      // 启动周期性同步 goroutine
      s.wg.Add(1)
      go s.syncLoop()
    }
  }

  return s
}

// MOD: 关闭方法，确保落盘并关闭 DB
func (s *StatsService) Close() error {
  if !s.storeEnabled || s.db == nil {
    // nothing to do
    return nil
  }
  // 停止 syncLoop
  close(s.stopCh)
  s.wg.Wait()
  // 最后一次强制落盘（确保内存最新值写入 DB）
  if err := s.persistAll(); err != nil {
    // 忽略错误返回，但可以记录
  }
  // 关闭 DB
  err := s.db.Close()
  s.db = nil
  return err
}

// MOD: 将 DB 中的 key 反向映射为内部计数器名
// 例如: v2ray_api.stats.inbounds.hysteria2-in.traffic.up -> inbound>>>hysteria2-in>>>traffic>>>uplink
func (s *StatsService) dbKeyToInternal(dbKey string) string {
  // 如果存在 file id 前缀，先去掉
  if s.storeFileID != "" {
    prefix := s.storeFileID + "::"
    if strings.HasPrefix(dbKey, prefix) {
      dbKey = strings.TrimPrefix(dbKey, prefix)
    }
  }
  // 如果已经是内部格式，直接返回
  if strings.Contains(dbKey, ">>>") {
    return dbKey
  }
  // 期望格式: v2ray_api.stats.<inbounds|outbounds|users>.<tag>.traffic.<up|down>
  parts := strings.Split(dbKey, ".")
  if len(parts) < 5 {
    // fallback: 把 . 替换为 >>> 保持兼容
    return strings.ReplaceAll(dbKey, ".", ">>>")
  }
  // parts example:
  // [v2ray_api, stats, inbounds, hysteria2-in, traffic, up]
  // find the segment that indicates type (inbounds/outbounds/users)
  var typIdx int = -1
  for i, p := range parts {
    if p == "inbounds" || p == "outbounds" || p == "users" {
      typIdx = i
      break
    }
  }
  if typIdx == -1 || typIdx+2 >= len(parts) {
    return strings.ReplaceAll(dbKey, ".", ">>>")
  }
  typ := parts[typIdx]
  tag := parts[typIdx+1]
  // direction is last part
  dir := parts[len(parts)-1]
  var t string
  switch typ {
  case "inbounds":
    t = "inbound"
  case "outbounds":
    t = "outbound"
  case "users":
    t = "user"
  default:
    t = typ
  }
  var d string
  switch dir {
  case "up":
    d = "uplink"
  case "down":
    d = "downlink"
  default:
    d = dir
  }
  // internal format: typ>>>tag>>>traffic>>>dir
  return t + ">>>" + tag + ">>>traffic>>>" + d
}

// MOD: 从 DB 恢复到 counters（改为反向映射并合并到内部计数器）
// 关键点：避免把 DB 的 v2ray_api.stats.* 作为独立计数器加载，改为映射到内部计数器名并累加
func (s *StatsService) loadFromDB() error {
  if s.db == nil {
    return nil
  }
  return s.db.View(func(tx *bolt.Tx) error {
    b := tx.Bucket([]byte(boltBucketName))
    if b == nil {
      return nil
    }
    c := b.Cursor()
    for k, v := c.First(); k != nil; k, v = c.Next() {
      rawKey := string(k)
      // 值为 int64 的二进制表示（使用 BigEndian）
      if len(v) != 8 {
        continue
      }
      var val int64
      val = int64(uint64(v[0])<<56 | uint64(v[1])<<48 | uint64(v[2])<<40 | uint64(v[3])<<32 |
        uint64(v[4])<<24 | uint64(v[5])<<16 | uint64(v[6])<<8 | uint64(v[7]))
      // MOD: 将 DB key 转为内部计数器名
      internalName := s.dbKeyToInternal(rawKey)
      s.access.Lock()
      counter, ok := s.counters[internalName]
      if !ok {
        // 创建并设置为 DB 值
        counter = &atomic.Int64{}
        counter.Store(val)
        s.counters[internalName] = counter
      } else {
        // 如果内存中已有计数器（极少数情况），则累加 DB 值以合并
        counter.Add(val)
      }
      // 记录 lastPersisted 使用内部计数器名（保证 persistChanged/persistAll 使用一致的键）
      s.lastPersisted[internalName] = counter.Load()
      s.access.Unlock()
    }
    return nil
  })
}

// MOD: 周期同步循环
func (s *StatsService) syncLoop() {
  defer s.wg.Done()
  ticker := time.NewTicker(s.storeInterval)
  defer ticker.Stop()
  for {
    select {
    case <-ticker.C:
      _ = s.persistChanged()
    case <-s.stopCh:
      return
    }
  }
}

// MOD: 将所有计数器写入 DB（用于关闭时的强制落盘）
// 注意：写入时使用 dbKeyFor 将内部计数器名映射为 DB key（保持原有行为）
func (s *StatsService) persistAll() error {
  if s.db == nil {
    return nil
  }
  s.persistLock.Lock()
  defer s.persistLock.Unlock()
  return s.db.Update(func(tx *bolt.Tx) error {
    b := tx.Bucket([]byte(boltBucketName))
    if b == nil {
      return nil
    }
    s.access.Lock()
    defer s.access.Unlock()
    for name, counter := range s.counters {
      val := counter.Load()
      key := s.dbKeyFor(name)
      // encode int64 -> 8 bytes big endian
      var buf [8]byte
      u := uint64(val)
      buf[0] = byte(u >> 56)
      buf[1] = byte(u >> 48)
      buf[2] = byte(u >> 40)
      buf[3] = byte(u >> 32)
      buf[4] = byte(u >> 24)
      buf[5] = byte(u >> 16)
      buf[6] = byte(u >> 8)
      buf[7] = byte(u)
      if err := b.Put([]byte(key), buf[:]); err != nil {
        return err
      }
      s.lastPersisted[name] = val
    }
    return nil
  })
}

// MOD: 仅写入发生变化的计数器，减少写盘频率与写量
func (s *StatsService) persistChanged() error {
  if s.db == nil {
    return nil
  }
  changed := make(map[string]int64)
  s.access.Lock()
  for name, counter := range s.counters {
    val := counter.Load()
    last, ok := s.lastPersisted[name]
    if !ok || val != last {
      changed[name] = val
    }
  }
  s.access.Unlock()
  if len(changed) == 0 {
    return nil
  }
  s.persistLock.Lock()
  defer s.persistLock.Unlock()
  return s.db.Update(func(tx *bolt.Tx) error {
    b := tx.Bucket([]byte(boltBucketName))
    if b == nil {
      return nil
    }
    for name, val := range changed {
      key := s.dbKeyFor(name)
      var buf [8]byte
      u := uint64(val)
      buf[0] = byte(u >> 56)
      buf[1] = byte(u >> 48)
      buf[2] = byte(u >> 40)
      buf[3] = byte(u >> 32)
      buf[4] = byte(u >> 24)
      buf[5] = byte(u >> 16)
      buf[6] = byte(u >> 8)
      buf[7] = byte(u)
      if err := b.Put([]byte(key), buf[:]); err != nil {
        return err
      }
      s.lastPersisted[name] = val
    }
    return nil
  })
}

// MOD: 将内部计数器名映射为 DB 中的持久化 key
// 内部计数器名示例: "inbound>>>tag>>>traffic>>>uplink"
// 目标格式: "v2ray_api.stats.inbounds.<tag>.traffic.up"
func (s *StatsService) dbKeyFor(internal string) string {
  parts := strings.Split(internal, ">>>")
  if len(parts) < 4 {
    key := strings.ReplaceAll(internal, ">>>", ".")
    if s.storeFileID != "" {
      return s.storeFileID + "::" + key
    }
    return key
  }
  typ := parts[0] // inbound/outbound/user
  tag := parts[1]
  dir := parts[3] // uplink/downlink
  var top string
  switch typ {
  case "inbound":
    top = "v2ray_api.stats.inbounds"
  case "outbound":
    top = "v2ray_api.stats.outbounds"
  case "user":
    top = "v2ray_api.stats.users"
  default:
    top = "v2ray_api.stats." + typ
  }
  var d string
  switch dir {
  case "uplink":
    d = "up"
  case "downlink":
    d = "down"
  default:
    d = dir
  }
  key := top + "." + tag + ".traffic." + d
  if s.storeFileID != "" {
    return s.storeFileID + "::" + key
  }
  return key
}

func (s *StatsService) RoutedConnection(ctx context.Context, conn net.Conn, metadata adapter.InboundContext, matchedRule adapter.Rule, matchOutbound adapter.Outbound) net.Conn {
	inbound := metadata.Inbound
	user := metadata.User
	outbound := matchOutbound.Tag()
	var readCounter []*atomic.Int64
	var writeCounter []*atomic.Int64
	countInbound := inbound != "" && s.inbounds[inbound]
	countOutbound := outbound != "" && s.outbounds[outbound]
	countUser := user != "" && s.users[user]
	if !countInbound && !countOutbound && !countUser {
		return conn
	}
	s.access.Lock()
	if countInbound {
		readCounter = append(readCounter, s.loadOrCreateCounter("inbound>>>"+inbound+">>>traffic>>>uplink"))
		writeCounter = append(writeCounter, s.loadOrCreateCounter("inbound>>>"+inbound+">>>traffic>>>downlink"))
	}
	if countOutbound {
		readCounter = append(readCounter, s.loadOrCreateCounter("outbound>>>"+outbound+">>>traffic>>>uplink"))
		writeCounter = append(writeCounter, s.loadOrCreateCounter("outbound>>>"+outbound+">>>traffic>>>downlink"))
	}
	if countUser {
		readCounter = append(readCounter, s.loadOrCreateCounter("user>>>"+user+">>>traffic>>>uplink"))
		writeCounter = append(writeCounter, s.loadOrCreateCounter("user>>>"+user+">>>traffic>>>downlink"))
	}
	s.access.Unlock()
	return bufio.NewInt64CounterConn(conn, readCounter, writeCounter)
}

func (s *StatsService) RoutedPacketConnection(ctx context.Context, conn N.PacketConn, metadata adapter.InboundContext, matchedRule adapter.Rule, matchOutbound adapter.Outbound) N.PacketConn {
	inbound := metadata.Inbound
	user := metadata.User
	outbound := matchOutbound.Tag()
	var readCounter []*atomic.Int64
	var writeCounter []*atomic.Int64
	countInbound := inbound != "" && s.inbounds[inbound]
	countOutbound := outbound != "" && s.outbounds[outbound]
	countUser := user != "" && s.users[user]
	if !countInbound && !countOutbound && !countUser {
		return conn
	}
	s.access.Lock()
	if countInbound {
		readCounter = append(readCounter, s.loadOrCreateCounter("inbound>>>"+inbound+">>>traffic>>>uplink"))
		writeCounter = append(writeCounter, s.loadOrCreateCounter("inbound>>>"+inbound+">>>traffic>>>downlink"))
	}
	if countOutbound {
		readCounter = append(readCounter, s.loadOrCreateCounter("outbound>>>"+outbound+">>>traffic>>>uplink"))
		writeCounter = append(writeCounter, s.loadOrCreateCounter("outbound>>>"+outbound+">>>traffic>>>downlink"))
	}
	if countUser {
		readCounter = append(readCounter, s.loadOrCreateCounter("user>>>"+user+">>>traffic>>>uplink"))
		writeCounter = append(writeCounter, s.loadOrCreateCounter("user>>>"+user+">>>traffic>>>downlink"))
	}
	s.access.Unlock()
	return bufio.NewInt64CounterPacketConn(conn, readCounter, nil, writeCounter, nil)
}

func (s *StatsService) GetStats(ctx context.Context, request *GetStatsRequest) (*GetStatsResponse, error) {
	s.access.Lock()
	counter, loaded := s.counters[request.Name]
	s.access.Unlock()
	if !loaded {
		return nil, E.New(request.Name, " not found.")
	}
	var value int64
	if request.Reset_ {
		value = counter.Swap(0)
	} else {
		value = counter.Load()
	}
	return &GetStatsResponse{Stat: &Stat{Name: request.Name, Value: value}}, nil
}

func (s *StatsService) QueryStats(ctx context.Context, request *QueryStatsRequest) (*QueryStatsResponse, error) {
	var response QueryStatsResponse
	s.access.Lock()
	defer s.access.Unlock()
	if len(request.Patterns) == 0 {
		for name, counter := range s.counters {
			var value int64
			if request.Reset_ {
				value = counter.Swap(0)
			} else {
				value = counter.Load()
			}
			response.Stat = append(response.Stat, &Stat{Name: name, Value: value})
		}
	} else if request.Regexp {
		matchers := make([]*regexp.Regexp, 0, len(request.Patterns))
		for _, pattern := range request.Patterns {
			matcher, err := regexp.Compile(pattern)
			if err != nil {
				return nil, err
			}
			matchers = append(matchers, matcher)
		}
		for name, counter := range s.counters {
			for _, matcher := range matchers {
				if matcher.MatchString(name) {
					var value int64
					if request.Reset_ {
						value = counter.Swap(0)
					} else {
						value = counter.Load()
					}
					response.Stat = append(response.Stat, &Stat{Name: name, Value: value})
				}
			}
		}
	} else {
		for name, counter := range s.counters {
			for _, matcher := range request.Patterns {
				if strings.Contains(name, matcher) {
					var value int64
					if request.Reset_ {
						value = counter.Swap(0)
					} else {
						value = counter.Load()
					}
					response.Stat = append(response.Stat, &Stat{Name: name, Value: value})
				}
			}
		}
	}
	return &response, nil
}

func (s *StatsService) GetSysStats(ctx context.Context, request *SysStatsRequest) (*SysStatsResponse, error) {
	var rtm runtime.MemStats
	runtime.ReadMemStats(&rtm)
	response := &SysStatsResponse{
		Uptime:       uint32(time.Since(s.createdAt).Seconds()),
		NumGoroutine: uint32(runtime.NumGoroutine()),
		Alloc:        rtm.Alloc,
		TotalAlloc:   rtm.TotalAlloc,
		Sys:          rtm.Sys,
		Mallocs:      rtm.Mallocs,
		Frees:        rtm.Frees,
		LiveObjects:  rtm.Mallocs - rtm.Frees,
		NumGC:        rtm.NumGC,
		PauseTotalNs: rtm.PauseTotalNs,
	}

	return response, nil
}

func (s *StatsService) mustEmbedUnimplementedStatsServiceServer() {
}

//nolint:staticcheck
func (s *StatsService) loadOrCreateCounter(name string) *atomic.Int64 {
	counter, loaded := s.counters[name]
	if loaded {
		return counter
	}
	counter = &atomic.Int64{}
	s.counters[name] = counter
  // MOD: 如果持久化已启用且 DB 中有历史值，则尝试从 lastPersisted 填充（loadFromDB 在初始化时已做）
  if s.storeEnabled {
    if v, ok := s.lastPersisted[name]; ok {
      counter.Store(v)
    }
  }
	return counter
}
