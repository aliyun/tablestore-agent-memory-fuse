module agent-memory-fuse

go 1.26.6

require (
	github.com/aliyun/tablestore-agent-storage-sdk/go v0.0.0-20260901040923-637f0435aceb
	github.com/hanwen/go-fuse/v2 v2.10.1
	golang.org/x/sys v0.28.0
	golang.org/x/text v0.39.0
)

require (
	github.com/aliyun/aliyun-oss-go-sdk v3.0.2+incompatible // indirect
	github.com/kr/pretty v0.3.1 // indirect
	golang.org/x/time v0.15.0 // indirect
)

replace github.com/aliyun/tablestore-agent-storage-sdk/go => ../tablestore-agent-storage-sdk/go
