package main

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
)

//如果当前目录下不存在目录Keys，则创建目录，并为各个节点生成rsa公私钥
func genRsaKeys(n int) {
	_ = os.RemoveAll("./Keys")

	fmt.Println("正在生成公私钥 ...")
	err := os.Mkdir("Keys", 0644) //创建目录,perm:权限
	if err != nil {
		log.Panic()
	}
	for i := 0; i <= n; i++ {
		if !isExist("./Keys/N" + strconv.Itoa(i)) { //将数字转换成对应的字符串类型的数字。
			err = os.Mkdir("./Keys/N"+strconv.Itoa(i), 0644) //创建文件夹
			if err != nil {
				log.Panic(err)
			}
		}

		priv, pub := getKeyPair()
		privFileName := "./Keys/N" + strconv.Itoa(i) + "/N" + strconv.Itoa(i) + "_RSA_PIV"
		err = ioutil.WriteFile(privFileName, priv, 0644)
		if err != nil {
			log.Panic(err)
		}

		pubFileName := "./Keys/N" + strconv.Itoa(i) + "/N" + strconv.Itoa(i) + "_RSA_PUB"
		err = ioutil.WriteFile(pubFileName, pub, 0644)
		if err != nil {
			log.Panic(err)
		}
	}
	fmt.Println("已为节点们生成RSA公私钥")
}

//生成rsa公私钥
func getKeyPair() (prvkey, pubkey []byte) {
	// 生成私钥文件
	//ReadFull从rand.Reader精确地读取len(b)字节数据填充进b
	//rand.Reader是一个全局、共享的密码用强随机数生成器
	privateKey, err := rsa.GenerateKey(rand.Reader, 1024)
	//GenerateKey使用随机源random(例如crypto/rand.Reader)生成给定位大小的RSA密钥对。
	if err != nil {
		panic(err)
	}
	// 定义公私钥格式
	derStream := x509.MarshalPKCS1PrivateKey(privateKey)
	block := &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: derStream,
	}
	prvkey = pem.EncodeToMemory(block)
	publicKey := &privateKey.PublicKey
	derPkix, err := x509.MarshalPKIXPublicKey(publicKey)
	if err != nil {
		panic(err)
	}
	block = &pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: derPkix,
	}
	pubkey = pem.EncodeToMemory(block)
	return
}

//判断文件或文件夹是否存在
func isExist(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		if os.IsNotExist(err) {
			return false
		}
		fmt.Println(err)
		return false
	}
	return true
}

//数字签名
func (p *PBFT) RsaSignWithSha256(data []byte, keyBytes []byte) []byte {
	h := sha256.New()
	h.Write(data)
	hashed := h.Sum(nil)
	block, _ := pem.Decode(keyBytes)
	if block == nil {
		panic(errors.New("private key error"))
	}
	privateKey, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		fmt.Println("ParsePKCS8PrivateKey err", err)
		panic(err)
	}

	signature, err := rsa.SignPKCS1v15(rand.Reader, privateKey, crypto.SHA256, hashed)
	if err != nil {
		fmt.Printf("Error from signing: %s\n", err)
		panic(err)
	}

	return signature
}

//签名验证
func (p *PBFT) RsaVerySignWithSha256(data, signData, keyBytes []byte) bool {
	block, _ := pem.Decode(keyBytes)
	if block == nil {
		panic(errors.New("public key error"))
	}
	pubKey, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		panic(err)
	}

	hashed := sha256.Sum256(data)
	err = rsa.VerifyPKCS1v15(pubKey.(*rsa.PublicKey), crypto.SHA256, hashed[:], signData)
	if err != nil {
		panic(err)
	}
	return true
}
