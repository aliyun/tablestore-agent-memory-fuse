// memctl 直连 Agent Memory 服务端的小工具：真实后端 e2e 时替代 mock 的控制
// 端点——列/建 store、绕过 FUSE 直接增删改查 item（制造资源版本冲突、检查服务
// 端返回的原始 JSON 形态）、批量种子与清理。
// 配置走环境变量：MEM_ENDPOINT MEM_AK MEM_SK MEM_INSTANCE MEM_STORE MEM_SCOPE
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aliyun/tablestore-agent-storage-sdk/go/agentstorage"
)

func env(key string) string { return os.Getenv(key) }

func itemPath(v string) string { return "/" + strings.Trim(v, "/") }

func listPrefix(v string) string {
	v = strings.Trim(v, "/")
	if v == "" {
		return ""
	}
	return "/" + v + "/"
}

func mustClient() *agentstorage.Client {
	cfg := agentstorage.Config{
		Endpoint:        env("MEM_ENDPOINT"),
		InstanceName:    env("MEM_INSTANCE"),
		AccessKeyID:     env("MEM_AK"),
		AccessKeySecret: env("MEM_SK"),
	}
	// MEM_SIGNV4=1 + MEM_REGION：专有/新地域可能要求 v4 签名
	if env("MEM_SIGNV4") == "1" {
		cfg.SignVersion = agentstorage.SignV4
		cfg.Region = env("MEM_REGION")
	}
	c, err := agentstorage.NewClient(cfg)
	if err != nil {
		die("client: %v", err)
	}
	return c
}

func die(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "memctl: "+format+"\n", args...)
	os.Exit(1)
}

func scoped() agentstorage.ItemScopedRequest {
	parts := strings.Split(env("MEM_SCOPE"), "#")
	if len(parts) != 4 {
		die("MEM_SCOPE must be app#tenant#agent#run")
	}
	return agentstorage.ItemScopedRequest{
		MemoryStoreName: env("MEM_STORE"),
		Scope:           agentstorage.Scope{AppID: parts[0], TenantID: parts[1], AgentID: parts[2], RunID: parts[3]},
	}
}

func dump(v any) {
	data, _ := json.MarshalIndent(v, "", "  ")
	fmt.Println(string(data))
}

// withRetry 对 409 LOCK_CONFLICT / 429 / 503 / 504 做有界退避重试，仅用于
// 真实 E2E 的测试数据准备和清理协调，不代表 FUSE mutation 会重放 write。
func withRetry(call func() error) error {
	backoff := []time.Duration{100 * time.Millisecond, 500 * time.Millisecond, time.Second}
	var err error
	for attempt := 0; ; attempt++ {
		err = call()
		if err == nil || attempt >= len(backoff) {
			return err
		}
		var serviceErr *agentstorage.ServiceError
		if !errors.As(err, &serviceErr) {
			return err
		}
		retryable := serviceErr.StatusCode == 429 || serviceErr.StatusCode == 503 || serviceErr.StatusCode == 504 ||
			(serviceErr.StatusCode == 409 && serviceErr.Code == "LOCK_CONFLICT")
		if !retryable {
			return err
		}
		time.Sleep(backoff[attempt])
	}
}

func main() {
	if len(os.Args) < 2 {
		die("usage: memctl stores|mkstore|rmstore|put|get|del|list|seed|purge ...")
	}
	command := os.Args[1]
	seedCount := 0
	switch command {
	case "stores":
		if len(os.Args) != 2 {
			die("usage: memctl stores")
		}
	case "mkstore", "rmstore", "get", "del", "purge":
		if len(os.Args) != 3 {
			die("usage: memctl %s <value>", command)
		}
	case "put":
		if len(os.Args) != 4 {
			die("usage: memctl put <path> <content>")
		}
	case "list":
		if len(os.Args) < 2 || len(os.Args) > 3 {
			die("usage: memctl list [prefix]")
		}
	case "seed":
		if len(os.Args) != 4 {
			die("usage: memctl seed <positive-count> <prefix>")
		}
		var err error
		seedCount, err = strconv.Atoi(os.Args[2])
		if err != nil || seedCount <= 0 {
			die("seed count must be a positive integer")
		}
	default:
		die("unknown subcommand %q", command)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	c := mustClient()

	switch command {
	case "stores":
		next := ""
		for {
			resp, err := c.ListMemoryStores(ctx, &agentstorage.ListMemoryStoresRequest{NextToken: next})
			if err != nil {
				die("ListMemoryStores: %v", err)
			}
			for _, s := range resp.Stores {
				info, err := c.GetMemoryStore(ctx, &agentstorage.GetMemoryStoreRequest{MemoryStoreName: s.MemoryStoreName})
				if err != nil {
					fmt.Printf("%s\t(GetMemoryStore: %v)\n", s.MemoryStoreName, err)
					continue
				}
				fmt.Printf("%s\tstorageMode=%s\n", info.MemoryStoreName, info.StorageMode)
			}
			if resp.NextToken == "" {
				break
			}
			next = resp.NextToken
		}
	case "mkstore": // memctl mkstore <name>
		resp, err := c.CreateMemoryStore(ctx, &agentstorage.CreateMemoryStoreRequest{
			MemoryStoreName: os.Args[2], Description: "agent-memory-fuse e2e test store", StorageMode: agentstorage.StorageModeFileMemory,
		})
		if err != nil {
			die("CreateMemoryStore: %v", err)
		}
		dump(resp)
	case "rmstore":
		if _, err := c.DeleteMemoryStore(ctx, &agentstorage.DeleteMemoryStoreRequest{MemoryStoreName: os.Args[2]}); err != nil {
			die("DeleteMemoryStore: %v", err)
		}
		fmt.Println("ok")
	case "put": // memctl put <path> <content>（upsert：PATH_EXISTS 时 Get+Update）
		path, content := itemPath(os.Args[2]), os.Args[3]
		var added *agentstorage.AddItemResponse
		err := withRetry(func() error {
			var err error
			added, err = c.AddItem(ctx, &agentstorage.AddItemRequest{
				ItemScopedRequest: scoped(), Path: path, Content: content, SessionID: "memctl", View: agentstorage.ItemViewFull,
			})
			return err
		})
		if err == nil {
			dump(added)
			return
		}
		var serviceErr *agentstorage.ServiceError
		if !errors.As(err, &serviceErr) || serviceErr.Code != "PATH_EXISTS" {
			die("AddItem: %v", err)
		}
		var updated *agentstorage.UpdateItemResponse
		err = withRetry(func() error {
			cur, err := c.GetItem(ctx, &agentstorage.GetItemRequest{
				ItemScopedRequest: scoped(), Path: path, View: agentstorage.ItemViewBasic,
			})
			if err != nil {
				return err
			}
			updated, err = c.UpdateItem(ctx, &agentstorage.UpdateItemRequest{
				ItemScopedRequest: scoped(), ItemID: cur.ItemID, Content: agentstorage.String(content),
				Precondition: &agentstorage.ItemPrecondition{ExpectedVersionID: cur.LatestVersionID},
				SessionID:    "memctl", View: agentstorage.ItemViewFull,
			})
			return err
		})
		if err != nil {
			die("put existing item: %v", err)
		}
		dump(updated)
	case "get": // 原始 JSON（检查 path 形态等协议细节）
		item, err := c.GetItem(ctx, &agentstorage.GetItemRequest{
			ItemScopedRequest: scoped(), Path: itemPath(os.Args[2]), View: agentstorage.ItemViewFull,
		})
		if err != nil {
			die("GetItem: %v", err)
		}
		dump(item)
	case "del":
		path := itemPath(os.Args[2])
		err := withRetry(func() error {
			cur, err := c.GetItem(ctx, &agentstorage.GetItemRequest{
				ItemScopedRequest: scoped(), Path: path, View: agentstorage.ItemViewBasic,
			})
			if err != nil {
				return err
			}
			_, err = c.DeleteItem(ctx, &agentstorage.DeleteItemRequest{
				ItemScopedRequest: scoped(), ItemID: cur.ItemID,
				Precondition: &agentstorage.ItemPrecondition{ExpectedVersionID: cur.LatestVersionID}, SessionID: "memctl",
			})
			return err
		})
		if err != nil {
			die("delete item: %v", err)
		}
		fmt.Println("ok")
	case "list": // memctl list [prefix]：打印 path 列表与总数
		prefix := ""
		if len(os.Args) > 2 {
			prefix = listPrefix(os.Args[2])
		}
		next, count := "", 0
		for {
			resp, err := c.ListItems(ctx, &agentstorage.ListItemsRequest{ItemScopedRequest: scoped(), PathPrefix: prefix, NextToken: next, Limit: 100})
			if err != nil {
				die("ListItems: %v", err)
			}
			for _, item := range resp.Items {
				fmt.Printf("%q\n", item.Path)
				count++
			}
			if resp.NextToken == "" {
				break
			}
			next = resp.NextToken
		}
		fmt.Printf("total=%d\n", count)
	case "seed": // memctl seed <n> <prefix>：并发 16 批量建文件
		n := seedCount
		prefix := strings.Trim(os.Args[3], "/")
		sem := make(chan struct{}, 16)
		var wg sync.WaitGroup
		var mu sync.Mutex
		errs := 0
		for i := 0; i < n; i++ {
			wg.Add(1)
			sem <- struct{}{}
			go func(i int) {
				defer wg.Done()
				defer func() { <-sem }()
				err := withRetry(func() error {
					_, err := c.AddItem(ctx, &agentstorage.AddItemRequest{
						ItemScopedRequest: scoped(), Path: itemPath(fmt.Sprintf("%s/f%04d.md", prefix, i)),
						Content: fmt.Sprintf("seed content %d", i), SessionID: "memctl",
					})
					return err
				})
				if err != nil {
					mu.Lock()
					errs++
					if errs <= 3 {
						fmt.Fprintf(os.Stderr, "seed %d: %v\n", i, err)
					}
					mu.Unlock()
				}
			}(i)
		}
		wg.Wait()
		fmt.Printf("seeded=%d errors=%d\n", n-errs, errs)
		if errs != 0 {
			os.Exit(1)
		}
	case "purge": // memctl purge <prefix>：删除该前缀的所有 item
		if strings.Trim(os.Args[2], "/") == "" {
			die("purge requires a non-empty prefix")
		}
		prefix := listPrefix(os.Args[2])
		var items []agentstorage.ItemListEntry
		next := ""
		for {
			resp, err := c.ListItems(ctx, &agentstorage.ListItemsRequest{ItemScopedRequest: scoped(), PathPrefix: prefix, NextToken: next, Limit: 100})
			if err != nil {
				die("ListItems: %v", err)
			}
			items = append(items, resp.Items...)
			if resp.NextToken == "" {
				break
			}
			next = resp.NextToken
		}
		sem := make(chan struct{}, 16)
		var wg sync.WaitGroup
		var mu sync.Mutex
		failed := 0
		for _, item := range items {
			wg.Add(1)
			sem <- struct{}{}
			go func(item agentstorage.ItemListEntry) {
				defer wg.Done()
				defer func() { <-sem }()
				err := withRetry(func() error {
					_, err := c.DeleteItem(ctx, &agentstorage.DeleteItemRequest{
						ItemScopedRequest: scoped(), ItemID: item.ItemID,
						Precondition: &agentstorage.ItemPrecondition{ExpectedVersionID: item.LatestVersionID}, SessionID: "memctl",
					})
					return err
				})
				if err != nil {
					mu.Lock()
					failed++
					mu.Unlock()
				}
			}(item)
		}
		wg.Wait()
		fmt.Printf("purged=%d failed=%d\n", len(items)-failed, failed)
		if failed != 0 {
			os.Exit(1)
		}
	}
}
