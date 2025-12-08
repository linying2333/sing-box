#!/bin/sh

export GOPROXY=http://127.0.0.1:7890,direct;
export http_proxy="http://127.0.0.1:7890"; 
export https_proxy="http://127.0.0.1:7890"; 
export ALL_PROXY="http://127.0.0.1:7890"; 
export GOMODCACHE="/e/Documents/GitHub/GoModCache"; 
export TMPDIR="/e/Temp"; 
export TEMP="/e/Temp"; 
export TMP="/e/Temp";

export CGO_ENABLED=0; 
export GOOS="windows"; 
export GOARCH="amd64";

# git pull
# git checkout tags/1.12.12

go build -trimpath -tags "with_quic with_dhcp with_wireguard with_utls with_acme with_clash_api with_v2ray_api with_gvisor with_tailscale" -ldflags "-s -w -X github.com/sagernet/sing-box/constant.Version=$(git describe --tags)" ./cmd/sing-box

ls sing-box*

# go clean -modcache