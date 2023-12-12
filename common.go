package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log"
)

//<REQUEST,o,t,errChan>
type Request struct {
	Message
	Timestamp int64
	//相当于clientID
	ClientAddr string
}

//<<PRE-PREPARE,v,n,d>,m>
type PrePrepareRequest struct {
	RequestMessage Request
	Digest         string
	SequenceID     int
	Sign           []byte
}

//<PREPARE,v,n,d,i>
type PrepareRequest struct {
	Digest     string
	SequenceID int
	NodeID     string
	Sign       []byte
}

//<COMMIT,v,n,D(m),i>
type CommitRequest struct {
	Digest     string
	SequenceID int
	NodeID     string
	Sign       []byte
}

type Response struct {
}

//<REPLY,v,t,errChan,i,r>
type Reply struct {
	MessageID int
	NodeID    string
	Result    bool
}

type Message struct {
	Content string
	ID      int
}

const prefixCMDLength = 12

type command string

const (
	cRequest    command = "request"
	cPrePrepare command = "preprepare"
	cPrepare    command = "prepare"
	cCommit     command = "commit"
)

//对消息详情进行摘要
func getDigest(request Request) string {
	b, err := json.Marshal(request)
	if err != nil {
		log.Panic(err)
	}
	hash := sha256.Sum256(b)
	//进行十六进制字符串编码
	return hex.EncodeToString(hash[:])
}
