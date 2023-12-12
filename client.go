package main

import (
	"bufio"
	"crypto/rand"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/rpc"
	"os"
	"strings"
	"time"
)

type Client struct {
	addr         string
	lis          net.Listener
	commitedChan chan int
	nodeCount    int
}

func NewClient(addr string, nodeCount int) *Client {
	return &Client{addr: addr, commitedChan: make(chan int, 128), nodeCount: nodeCount}
}

func (c *Client) Close() {
	_ = c.lis.Close()
}

func (c *Client) Start() {
	go c.listen()
	c.command()
}

func (c *Client) Reply(args int, resp *struct{}) error {
	c.commitedChan <- args
	return nil
}

func (c *Client) listen() {
	lis, err := net.Listen("tcp", c.addr) //监听端口，启动服务
	if err != nil {
		log.Panic(err)
	}
	c.lis = lis
	srv := rpc.NewServer()
	if err = srv.RegisterName("client", c); err != nil {
		log.Panic(err)
	}
	srv.Accept(lis)
}

func (c *Client) probe(expectID int, done chan time.Duration) {
	fmt.Println("开始等待共识: ", expectID)
	ack := c.nodeCount / 3 * 2
	since := time.Now()
	for {
		select {
		case actualID := <-c.commitedChan:
			if actualID == expectID {
				ack--
				//fmt.Println("剩余共识节点: ", ack)
				if ack == 0 {
					fmt.Println("共识结束")
					goto DONE
				}
			}
			//} else {
			//	fmt.Printf("Warn: unexpect actualID, expectID:%d actual:%d\n", actualID, expectID)
			//}
		}
	}
DONE:
	used := time.Now().Sub(since)
	fmt.Printf("time used: %dms\n", used.Milliseconds())
	done <- used
}

func (c *Client) sendToPrimaryNode(addr string, req *Request) {
	cli, err := rpc.Dial("tcp", addr)
	if err != nil {
		panic(err)
	}
	defer cli.Close()
	if err = cli.Call("pbft.ClientRequest", req, &Response{}); err != nil {
		panic(err)
	}
}

func (c *Client) command() {
	fmt.Printf("客户端开启监听，地址：%s\n", c.addr)
	fmt.Println(" ---------------------------------------------------------------------------------")
	fmt.Println("|  已进入PBFT测试Demo客户端，请启动全部节点后再发送消息！ :)  |")
	fmt.Println(" ---------------------------------------------------------------------------------")
	fmt.Println("请在下方输入要存入节点的信息：")

	//首先通过命令行获取用户输入
	stdReader := bufio.NewReader(os.Stdin)
	for {
		data, err := stdReader.ReadString('\n') //ReadString读取直到第一次遇到delim字节，返回一个包含已读取的数据和delim字节的字符串
		if err != nil {
			if err == io.EOF {
				return
			}
			fmt.Println("Error reading from stdin")
			panic(err)
		}

		szUsedTime := make([]time.Duration, 0)
		for i := 0; i < 20; i++ {
			fmt.Println("执行：", i+1)
			req := &Request{
				Timestamp:  time.Now().UnixNano(), //当前时间戳
				ClientAddr: c.addr,
				Message: Message{
					ID:      c.getRandom(),
					Content: strings.TrimSpace(data), //把输入的data转换为string类型的切片
				},
			}

			done := make(chan time.Duration, 1)
			go c.probe(req.Message.ID, done)
			c.sendToPrimaryNode(nodeTable["N0"], req) //默认N0为主节点，直接把请求信息发送至N0
			szUsedTime = append(szUsedTime, <-done)
		}

		fmt.Println("共识用时：")
		count := int64(0)
		max := int64(0)
		min := int64(0x0FFFFFFF)
		for i := 0; i < len(szUsedTime); i++ {
			used := szUsedTime[i].Milliseconds()
			fmt.Printf("%2d: %dms\n", i+1, used)
			if used > max {
				max = used
			}
			if used < min {
				min = used
			}
			count += used
		}
		fmt.Printf("总用时:%dms\t平均用时:%dms\t最大用时：%dms\t最小用时:%dms\n",
			count, (count-max-min)/(int64(len(szUsedTime))-2), max, min)
	}
}

//返回一个十位数的随机数，作为msgid
func (c *Client) getRandom() int {
	x := big.NewInt(10000000000)
	for {
		result, err := rand.Int(rand.Reader, x)
		if err != nil {
			log.Panic(err)
		}
		if result.Int64() > 1000000000 {
			return int(result.Int64())
		}
	}
}
