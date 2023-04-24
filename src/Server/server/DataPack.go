package server

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"strconv"
)

type DataPack struct {
	conn net.Conn
	eof  string
}

func NewDataPack(conn net.Conn) *DataPack {
	return &DataPack{conn, "\r\n"}
}

func (this *DataPack) PackMsg(msg [][]byte) {
	this.packArray(msg)
}

func (this *DataPack) packArray(msg [][]byte) {
	num := len(msg)
	str := fmt.Sprintf("*%d%s", num, this.eof)
	// fmt.Printf(str)
	this.conn.Write([]byte(str))
	for i := 0; i < num; i++ {
		this.packBinary(msg[i])
	}
}

func (this *DataPack) packBinary(elem []byte) {
	len := len(elem)
	str := fmt.Sprintf("$%d%s", len, this.eof)
	this.conn.Write([]byte(str))
	this.conn.Write(elem)
	this.conn.Write([]byte(this.eof))
	// fmt.Printf(str)
	// fmt.Printf(string(elem))
	// fmt.Printf(this.eof)
}

func (this *DataPack) UnpackMsg() [][]byte {
	reader := bufio.NewReader(this.conn)
	sign, err := reader.ReadByte()
	if err != nil {
		panic(err.Error())
	}
	switch sign {
	case byte('*'):
		return this.unpackArray(reader)
	default:
		panic("invalid msg to unpack")
	}
}

func (this *DataPack) unpackArray(reader *bufio.Reader) [][]byte {
	data, err := reader.ReadBytes('\n')
	if err != nil {
		panic(err.Error())
	}
	data = bytes.TrimRight(data, this.eof)
	num, err := strconv.Atoi(string(data))
	if err != nil {
		panic(err.Error())
	}

	ret := make([][]byte, num)
	for i := 0; i < num; i++ {
		sign, err := reader.ReadByte()
		if (err != nil) || (sign != byte('$')) {
			panic("invalid msg")
		}
		elem := this.unpackBinary(reader)
		ret[i] = elem
	}
	return ret
}

func (this *DataPack) unpackBinary(reader *bufio.Reader) []byte {
	data, err := reader.ReadBytes('\n')
	if err != nil {
		panic(err.Error())
	}
	data = bytes.TrimRight(data, this.eof)
	num, err := strconv.Atoi(string(data))
	if err != nil {
		panic(err.Error())
	}
	data = make([]byte, num)
	if _, err = reader.Read(data); err != nil {
		panic(err.Error())
	}
	// 读取\r\n
	if _, err = reader.ReadBytes('\n'); err != nil {
		panic(err.Error())
	}
	return data
}
