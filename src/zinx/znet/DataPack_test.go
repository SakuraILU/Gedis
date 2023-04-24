package znet

import (
	"encoding/binary"
	"testing"
)

// test data pack

// test: build msg and pack, msg is len uint32 + id uint32 + data []byte
func TestDataPack_Pack(t *testing.T) {
	dp := NewDataPack()
	str := "zinx v0.5 client test msg"
	msg := NewMessage(0, []byte(str))
	data, err := dp.Pack(msg)
	if err != nil {
		t.Error("Pack error: ", err)
		return
	}
	// assert msg len
	if len(data) != 8+len(str) {
		t.Error("Pack error: msg len is not correct")
		return
	}
	// assert msg id
	if binary.LittleEndian.Uint32(data[4:8]) != 0 {
		t.Error("Pack error: msg id is not correct")
		return
	}
	// assert msg data
	if string(data[8:]) != "zinx v0.5 client test msg" {
		t.Error("Pack error: msg data is not correct")
		return
	}
}

// test: unpack msg head
func TestDataPack_UnpackHead(t *testing.T) {
	dp := NewDataPack()
	str := "zinx v0.5 client test msg"
	msg := NewMessage(0, []byte(str))
	data, err := dp.Pack(msg)
	if err != nil {
		t.Error("Pack error: ", err)
		return
	}
	// unpack head
	msg_head, err := dp.UnpackHead(data)
	if err != nil {
		t.Error("UnpackHead error: ", err)
		return
	}
	// assert msg len
	if msg_head.GetDataLen() != uint32(len(str)) {
		t.Error("UnpackHead error: msg len is not correct")
		return
	}
	// assert msg id
	if msg_head.GetMsgID() != 0 {
		t.Error("UnpackHead error: msg id is not correct")
		return
	}
}

// test: unpack msg head and extract body
func TestDataPack_Unpack(t *testing.T) {
	dp := NewDataPack()
	str := "zinx v0.5 client test msg"
	msg := NewMessage(0, []byte(str))
	data, err := dp.Pack(msg)
	if err != nil {
		t.Error("Pack error: ", err)
		return
	}
	// unpack msg head
	msg_head, err := dp.UnpackHead(data)
	if err != nil {
		t.Error("UnpackHead error: ", err)
		return
	}
	// assert msg len
	if msg_head.GetDataLen() != uint32(len(str)) {
		t.Error("UnpackHead error: msg len is not correct")
		return
	}
	// assert msg id
	if msg_head.GetMsgID() != 0 {
		t.Error("UnpackHead error: msg id is not correct")
		return
	}
	// extract msg data in [GetMsgLen, GetMsgLen+GetMsgLen]
	msg_data := data[8 : 8+msg_head.GetDataLen()]
	// assert msg_data is correct
	if string(msg_data) != str {
		t.Error("UnpackHead error: msg data is not correct")
		return
	}
}

// test multi pack and unpack in one buffer [msg1 | msg2]
func TestDataPack_MultiPack(t *testing.T) {
	dp := NewDataPack()
	str1 := "zinx v0.5 client test msg1"
	msg1 := NewMessage(0, []byte(str1))
	data1, err := dp.Pack(msg1)
	if err != nil {
		t.Error("Pack error: ", err)
		return
	}
	str2 := "zinx v0.5 client test msg2"
	msg2 := NewMessage(0, []byte(str2))
	data2, err := dp.Pack(msg2)
	if err != nil {
		t.Error("Pack error: ", err)
		return
	}
	// combine data1 and data2
	data := append(data1, data2...)
	// unpack msg1
	msg_head, err := dp.UnpackHead(data)
	if err != nil {
		t.Error("UnpackHead error: ", err)
		return
	}
	// assert msg len
	if msg_head.GetDataLen() != uint32(len(str1)) {
		t.Error("UnpackHead error: msg len is not correct")
		return
	}
	// assert msg id
	if msg_head.GetMsgID() != 0 {
		t.Error("UnpackHead error: msg id is not correct")
		return
	}
	// extract msg data in [GetMsgLen, GetMsgLen+GetMsgLen]
	msg_data := data[8 : 8+msg_head.GetDataLen()]
	// assert msg_data is correct
	if string(msg_data) != str1 {
		t.Error("UnpackHead error: msg data is not correct")
		return
	}
	// unpack msg2
	msg_head, err = dp.UnpackHead(data[8+msg_head.GetDataLen():])
	if err != nil {
		t.Error("UnpackHead error: ", err)
		return
	}
	// assert msg len
	if msg_head.GetDataLen() != uint32(len(str2)) {
		t.Error("UnpackHead error: msg len is not correct")
		return
	}
	// assert msg id
	if msg_head.GetMsgID() != 0 {
		t.Error("UnpackHead error: msg id is not correct")
		return
	}
	// extract msg data in [GetMsgLen, GetMsgLen+GetMsgLen]
	msg_data = data[8+msg_head.GetDataLen()+8 : 8+msg_head.GetDataLen()+8+msg_head.GetDataLen()]
	// assert msg_data is correct
	if string(msg_data) != str2 {
		t.Error("UnpackHead error: msg data is not correct")
		return
	}
}
