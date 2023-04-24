package server

import (
	"bufio"
	"bytes"
	"fmt"
	"strconv"
)

type CmdPack struct {
	eof string
}

func NewCmdPack() *CmdPack {
	return &CmdPack{eof: "\r\n"}
}

func (this *CmdPack) PackCmd(msg [][]byte) []byte {
	return this.packArray(msg)
}

func (this *CmdPack) packArray(msg [][]byte) []byte {
	num := len(msg)
	str := fmt.Sprintf("*%d%s", num, this.eof)
	buf := make([]byte, 0)
	buf = append(buf, []byte(str)...)
	for i := 0; i < num; i++ {
		binary_buf := this.packBinary(msg[i])
		buf = append(buf, binary_buf...)
	}
	return buf
}

func (this *CmdPack) packBinary(elem []byte) []byte {
	len := len(elem)
	str := fmt.Sprintf("$%d%s", len, this.eof)
	buf := make([]byte, 0)
	buf = append(buf, []byte(str)...)
	buf = append(buf, elem...)
	buf = append(buf, []byte(this.eof)...)

	return buf
}

func (this *CmdPack) UnpackCmd(buf []byte) [][]byte {
	reader := bufio.NewReader(bytes.NewReader(buf))
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

func (this *CmdPack) unpackArray(reader *bufio.Reader) [][]byte {
	bnum, err := reader.ReadBytes('\n')
	bnum = bytes.TrimRight(bnum, this.eof)
	num, err := strconv.Atoi(string(bnum))
	if err != nil {
		panic(err.Error())
	}

	ret := make([][]byte, num)
	for i := 0; i < num; i++ {
		sign, err := reader.ReadByte()
		if (err != nil) || (sign != byte('$')) {
			panic("invalid msg")
		}
		ret[i] = this.unpackBinary(reader)
	}
	return ret
}

func (this *CmdPack) unpackBinary(reader *bufio.Reader) []byte {
	bnum, err := reader.ReadBytes('\n')
	if err != nil {
		panic(err.Error())
	}
	bnum = bytes.TrimRight(bnum, this.eof)
	num, err := strconv.Atoi(string(bnum))
	if err != nil {
		panic(err.Error())
	}
	data := make([]byte, num)
	if _, err = reader.Read(data); err != nil {
		panic(err.Error())
	}
	// 读取\r\n
	if _, err = reader.ReadBytes('\n'); err != nil {
		panic(err.Error())
	}
	return data
}
