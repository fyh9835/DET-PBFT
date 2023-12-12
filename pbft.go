package main

import (
	"encoding/hex"
	"fmt"
	"github.com/pkg/errors"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"
)

var (
	ErrRefuse = errors.New("refuse")
	SleepTime = time.Millisecond * 5
)

//本地消息池（模拟持久化层），只有确认提交成功后才会存入此池
var localMessagePool sync.Map

type Node struct {
	//节点ID
	nodeID string
	//节点监听地址
	addr string
	//RSA私钥
	rsaPrivKey []byte
	//RSA公钥
	rsaPubKey []byte
}

type PBFT struct {
	lis           net.Listener
	locker        sync.Mutex
	nodeClientMap sync.Map
	clientMap     sync.Map
	nodeCount     int
	finish        bool

	//节点信息
	node Node
	//每笔请求自增序号
	sequenceID int
	//临时消息池，消息摘要对应消息本体
	messagePool map[string]Request
	//存放收到的prepare数量(至少需要收到并确认2f个)，根据摘要来对应
	prepareConfirmCount map[string]map[string]bool
	//存放收到的commit数量（至少需要收到并确认2f+1个），根据摘要来对应
	commitConfirmCount map[string]map[string]bool
	//该笔消息是否已进行Commit广播
	isCommitBroadcast map[string]bool
	//该笔消息是否已对客户端进行Reply
	isReply map[string]bool

	clientReqChan     chan *Request
	prePrepareReqChan chan *PrePrepareRequest
	prepareReqChan    chan *PrepareRequest
	commitReqChan     chan *CommitRequest
	stopped           chan struct{}
}

//初始化共识节点
func NewPBFT(nodeID, addr string, nodeCount int) *PBFT { //N0,N1,N2,N3; port
	p := new(PBFT)
	p.nodeCount = nodeCount
	p.node.nodeID = nodeID
	p.node.addr = addr
	p.node.rsaPrivKey = p.getPivKey(nodeID) //从生成的私钥文件处读取
	p.node.rsaPubKey = p.getPubKey(nodeID)  //从生成的公钥文件处读取
	p.sequenceID = 1
	p.messagePool = make(map[string]Request)
	p.prepareConfirmCount = make(map[string]map[string]bool)
	p.commitConfirmCount = make(map[string]map[string]bool)
	p.isCommitBroadcast = make(map[string]bool)
	p.isReply = make(map[string]bool)

	p.finish = true

	p.clientReqChan = make(chan *Request, 8192)
	p.prePrepareReqChan = make(chan *PrePrepareRequest, 8192)
	p.prepareReqChan = make(chan *PrepareRequest, 8192)
	p.commitReqChan = make(chan *CommitRequest, 8192)
	p.stopped = make(chan struct{}, 1)
	return p
}

func (p *PBFT) Close() {
	p.nodeClientMap.Range(func(key, value any) bool {
		_ = value.(*rpc.Client).Close()
		return true
	})
	p.clientMap.Range(func(key, value any) bool {
		_ = value.(*rpc.Client).Close()
		return true
	})
	close(p.stopped)
	_ = p.lis.Close()
	fmt.Printf("[%s] done\n", p.node.nodeID)
}

// Start 节点使用的tcp监听
func (p *PBFT) Start() {
	lis, err := net.Listen("tcp", p.node.addr) //服务端监听
	if err != nil {
		log.Panic(err)
	}
	p.lis = lis
	rpcSrv := rpc.NewServer()
	if err = rpcSrv.RegisterName("pbft", p); err != nil {
		panic(err)
	}
	go p.process()
	fmt.Printf("节点%s开启监听，地址：%s\n", p.node.nodeID, p.node.addr)
	rpcSrv.Accept(lis)
}

func (p *PBFT) process() {
	for {
		select {
		case req := <-p.clientReqChan:
			if err := p.handleClientRequest(req); err == ErrRefuse {
				go func() {
					time.Sleep(SleepTime)
					p.clientReqChan <- req
				}()
			}
		case req := <-p.prePrepareReqChan:
			if req.SequenceID < p.sequenceID {
				continue
			}
			err := p.handlePrePrepare(req)
			if err != nil {
				if err == ErrRefuse {
					go func() {
						time.Sleep(SleepTime)
						p.prePrepareReqChan <- req
					}()
				} else {
					fmt.Printf("[ERROR][PrePrepare] %v\n", err)
				}
			}
		case req := <-p.prepareReqChan:
			if req.SequenceID < p.sequenceID {
				//fmt.Println("!!!!!!", req.SequenceID, p.sequenceID)
				continue
			}
			err := p.handlePrepare(req)
			if err != nil {
				if err == ErrRefuse {
					go func() {
						time.Sleep(SleepTime)
						p.prepareReqChan <- req
					}()
				} else {
					fmt.Printf("[ERROR][Prepare] %v\n", err)
				}
			}
		case req := <-p.commitReqChan:
			if req.SequenceID < p.sequenceID {
				continue
			}
			err := p.handleCommit(req)
			if err != nil {
				if err == ErrRefuse {
					go func() {
						time.Sleep(SleepTime)
						p.commitReqChan <- req
					}()
				} else {
					fmt.Printf("[ERROR][Commit] %v\n", err)
				}
			}
		case <-p.stopped:
			return
		}
	}
}

func (p *PBFT) handleClientRequest(req *Request) error {
	if p.finish == false {
		return ErrRefuse
	}
	p.finish = false

	digest := getDigest(*req)    //获取消息摘要
	p.sequenceID++               //添加信息序号
	p.messagePool[digest] = *req //存入临时消息池

	// 广播PrePrepare
	//fmt.Println("broadcast PrePrepare...")
	digestByte, _ := hex.DecodeString(digest)
	sig := p.RsaSignWithSha256(digestByte, p.node.rsaPrivKey)
	ppReq := &PrePrepareRequest{*req, digest, p.sequenceID, sig}
	p.broadcast(true, func(cli *rpc.Client) error {
		err := cli.Call("pbft.PrePrepare", ppReq, &Response{})
		if err != nil {
			panic(err)
		}
		return nil
	})

	//fmt.Printf("%s broadcast PrePrepare \n", p.node.nodeID)
	return nil
}

func (p *PBFT) handlePrePrepare(req *PrePrepareRequest) error {
	//fmt.Printf("%s receive PrePrepare ...\n", p.node.nodeID)

	// 验签
	if req.Digest != getDigest(req.RequestMessage) {
		return errors.New("信息摘要对不上，拒绝进行prepare广播")
	}
	digestByte, _ := hex.DecodeString(req.Digest)
	if !p.RsaVerySignWithSha256(digestByte, req.Sign, p.getPubKey("N0")) {
		return errors.New("主节点签名验证失败！,拒绝进行prepare广播")
	}

	if _, ok := p.messagePool[req.Digest]; ok {
		return nil
	}
	if p.sequenceID+1 != req.SequenceID {
		//fmt.Println("Warn: 消息序号对不上，拒绝进行prepare广播\"")
		return ErrRefuse
	}
	p.sequenceID = req.SequenceID
	p.messagePool[req.Digest] = req.RequestMessage //将信息存入临时消息池

	// 广播Prepare
	//fmt.Println("broadcast Prepare...")
	// 签名
	sign := p.RsaSignWithSha256(digestByte, p.node.rsaPrivKey)
	preReq := &PrepareRequest{req.Digest, req.SequenceID, p.node.nodeID, sign}
	p.broadcast(false, func(cli *rpc.Client) error {
		return cli.Call("pbft.Prepare", preReq, &Response{})
	})

	//fmt.Printf("%s broadcast Prepare \n", p.node.nodeID)

	return nil
}

func (p *PBFT) handlePrepare(req *PrepareRequest) error {
	//fmt.Printf("%s receive %s Prepare ... \n", p.node.nodeID, req.NodeID)

	digestByte, _ := hex.DecodeString(req.Digest)
	if !p.RsaVerySignWithSha256(digestByte, req.Sign, p.getPubKey(req.NodeID)) {
		return errors.New("节点签名验证失败！,拒绝执行commit广播")
	}

	if true == p.isCommitBroadcast[req.Digest] {
		return nil
	}
	if _, ok := p.messagePool[req.Digest]; !ok {
		//fmt.Println("Warn: 当前临时消息池无此摘要，拒绝执行commit广播")
		return ErrRefuse
	}
	if p.sequenceID != req.SequenceID {
		//fmt.Println("Warn: 消息序号对不上，拒绝执行commit广播")
		return ErrRefuse
	}
	p.setPrepareConfirmMap(req.Digest, req.NodeID, true) // 记录收到了哪个节点的Prepare消息
	count := len(p.prepareConfirmCount[req.Digest])

	//因为主节点不会发送Prepare，所以不包含自己
	specifiedCount := 0
	if p.node.nodeID == "N0" {
		specifiedCount = p.nodeCount / 3 * 2
	} else {
		specifiedCount = (p.nodeCount / 3 * 2) - 1
	}

	if count >= specifiedCount {
		p.isCommitBroadcast[req.Digest] = true
		//如果节点至少收到了2f个prepare的消息（包括自己）,并且没有进行过commit广播，则进行commit广播
		//fmt.Println("broadcast Commit...")
		sign := p.RsaSignWithSha256(digestByte, p.node.rsaPrivKey)
		cReq := &CommitRequest{req.Digest, req.SequenceID, p.node.nodeID, sign}
		p.broadcast(false, func(cli *rpc.Client) error {
			return cli.Call("pbft.Commit", cReq, &Response{})
		})
		//fmt.Printf("%s broadcast Commit \n", p.node.nodeID)
	}

	return nil
}

func (p *PBFT) handleCommit(req *CommitRequest) error {
	//fmt.Printf("%s receive %s Commit ... \n", p.node.nodeID, req.NodeID)

	digestByte, _ := hex.DecodeString(req.Digest)
	if !p.RsaVerySignWithSha256(digestByte, req.Sign, p.getPubKey(req.NodeID)) {
		return errors.New("节点签名验证失败！,拒绝将信息持久化到本地消息池")
	}

	if false == p.isCommitBroadcast[req.Digest] {
		return ErrRefuse
	}
	if p.isReply[req.Digest] {
		return nil
	}
	if p.sequenceID != req.SequenceID {
		//fmt.Printf("Warn: 消息序号[local:%d remote:%d]对不上，拒绝将信息持久化到本地消息池\n", p.sequenceID, req.SequenceID)
		return ErrRefuse
	}
	if _, ok := p.prepareConfirmCount[req.Digest]; !ok {
		//fmt.Println("Warn: 当前prepare池无此摘要，拒绝将信息持久化到本地消息池")
		return ErrRefuse
	}

	p.setCommitConfirmMap(req.Digest, req.NodeID, true)
	count := len(p.commitConfirmCount[req.Digest])
	if count < p.nodeCount/3*2 {
		//fmt.Println("*******************")
		return nil
	}
	//如果节点至少收到了2f+1个commit消息（包括自己）,并且节点没有回复过,并且已进行过commit广播，则提交信息至本地消息池，并reply成功标志至客户端！
	localMessagePool.Store(req.Digest, struct{}{})
	//localMessagePool = append(localMessagePool, p.messagePool[req.Digest].Message)
	info := p.messagePool[req.Digest].ID
	p.isReply[req.Digest] = true
	p.finish = true

	//fmt.Printf("reply client %d...\n", info)
	p.sendToClient(p.messagePool[req.Digest].ClientAddr, info)

	//fmt.Printf("%s committed \n", p.node.nodeID)
	return nil
}

//ClientRequest 处理客户端发来的请求
func (p *PBFT) ClientRequest(req *Request, resp *Response) error {
	p.clientReqChan <- req
	return nil
}

//PrePrepare 处理预准备消息
func (p *PBFT) PrePrepare(req *PrePrepareRequest, resp *Response) error {
	p.prePrepareReqChan <- req
	return nil
}

//Prepare 处理准备消息
func (p *PBFT) Prepare(req *PrepareRequest, resp *Request) error {
	p.prepareReqChan <- req
	return nil
}

//Commit 处理提交确认消息
func (p *PBFT) Commit(req *CommitRequest, resp *Response) error {
	p.commitReqChan <- req
	return nil
}

//向除自己外的其他节点进行广播
func (p *PBFT) broadcast(isSync bool, fn func(cli *rpc.Client) error) {
	if isSync {
		for id, addr := range nodeTable {
			if id == p.node.nodeID {
				continue
			}
			if err := fn(p.getNodeClient(addr)); err != nil {
				panic(err)
			}
		}
	} else {
		for id, addr := range nodeTable {
			if id == p.node.nodeID {
				continue
			}
			go func(addr string) {
				if err := fn(p.getNodeClient(addr)); err != nil {
					panic(err)
				}
			}(addr)
		}
	}

}

func (p *PBFT) getNodeClient(addr string) *rpc.Client {
	p.locker.Lock()
	defer p.locker.Unlock()
	v, ok := p.nodeClientMap.Load(addr)
	if ok {
		return v.(*rpc.Client)
	}
	cli, err := rpc.Dial("tcp", addr)
	if err != nil {
		panic(err)
	}
	p.nodeClientMap.Store(addr, cli)
	return cli
}

func (p *PBFT) sendToClient(addr string, id int) {
	p.locker.Lock()
	var cli *rpc.Client
	v, ok := p.clientMap.Load(addr)
	if !ok {
		var err error
		cli, err = rpc.Dial("tcp", addr)
		if err != nil {
			panic(err)
		}
		p.clientMap.Store(addr, cli)
	} else {
		cli = v.(*rpc.Client)
	}
	p.locker.Unlock()

	err := cli.Call("client.Reply", id, &struct{}{})
	if err != nil {
		panic(err)
	}
}

//为多重映射开辟赋值
func (p *PBFT) setPrepareConfirmMap(val, val2 string, b bool) {
	if _, ok := p.prepareConfirmCount[val]; !ok {
		p.prepareConfirmCount[val] = make(map[string]bool)
	}
	p.prepareConfirmCount[val][val2] = b
}

//为多重映射开辟赋值
func (p *PBFT) setCommitConfirmMap(val, val2 string, b bool) {
	if _, ok := p.commitConfirmCount[val]; !ok {
		p.commitConfirmCount[val] = make(map[string]bool)
	}
	p.commitConfirmCount[val][val2] = b
}

//传入节点编号， 获取对应的公钥
func (p *PBFT) getPubKey(nodeID string) []byte {
	key, err := ioutil.ReadFile("Keys/" + nodeID + "/" + nodeID + "_RSA_PUB")
	if err != nil {
		log.Panic(err)
	}
	return key
}

//传入节点编号， 获取对应的私钥
func (p *PBFT) getPivKey(nodeID string) []byte {
	key, err := ioutil.ReadFile("Keys/" + nodeID + "/" + nodeID + "_RSA_PIV")
	if err != nil {
		log.Panic(err)
	}
	return key
}
