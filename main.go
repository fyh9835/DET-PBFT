package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
)

//节点池，主要用来存储监听地址
var nodeTable map[string]string

// go run main.go -nodeClientMap "N0;127.0.0.1:8000,N1"
func main() {
	typ := flag.String("type", "client", "start type")
	n := flag.Int("n", 10, "nodeClientMap count")
	flag.Parse()

	nodeTable = map[string]string{}
	port := 18000
	for i := 0; i < *n; i++ {
		nodeTable[fmt.Sprintf("N%d", i)] = fmt.Sprintf("127.0.0.1:%d", port)
		port++
	}

	nodes := make([]*PBFT, 0)
	var cli *Client
	switch *typ {
	case "client":
		cli = NewClient("127.0.0.1:18889", *n)
		go cli.Start() //启动客户端程序
	case "node":
		// 生成公私钥---用来签名
		genRsaKeys(*n)
		for nodeID, addr := range nodeTable {
			p := NewPBFT(nodeID, addr, *n) //初始化共识节点
			go p.Start()                   //启动节点
			nodes = append(nodes, p)
		}
	default:
		panic("unknown")
	}

	// graceful stop
	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, os.Kill)
	<-done

	fmt.Println("准备退出...")
	switch *typ {
	case "client":
		if cli != nil {
			cli.Close()
			fmt.Println("关闭客户端成功")
		}
	case "node":
		for _, p := range nodes {
			p.Close()
		}
		fmt.Println("关闭节点成功")
	}
}
