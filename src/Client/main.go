package main

import (
	"bufio"
	"fmt"
	"gedis/src/Server/server"
	"gedis/src/zinx/znet"
	"net"
	"os"
	"strconv"
	"strings"
)

var prompt = "Gedis"
var validCmds = []string{"SET", "GET", "DEL", "MSET", "EXPIRE", "TTL", "KEYS", "PERSIST"}
var validDbCmds = []string{"SELECT"}

func reader(conn net.Conn) {
	msg_packer := znet.NewDataPack()
	cmd_packer := server.NewCmdPack()
	db_id := 0
	for {
		fmt.Printf("%s[%d]> ", prompt, db_id)
		buf := make([]byte, msg_packer.GetHeadLen())
		_, err := conn.Read(buf)
		if err != nil {
			panic(err.Error())
		}
		msg, err := msg_packer.UnpackHead(buf)
		if err != nil {
			panic(err.Error())
		}
		_, err = conn.Read(msg.GetMsgData())
		if err != nil {
			panic(err.Error())
		}
		cmd_packed := msg.GetMsgData()
		cmds := cmd_packer.UnpackCmd(cmd_packed)
		if msg.GetMsgID() == 1 {
			db_id, err = strconv.Atoi(string(cmds[0]))
			fmt.Printf("\"OK\"\n")
		} else {
			for _, v := range cmds {
				fmt.Printf("\"%s\"\n", v)
			}
		}
	}
}

func writer(conn net.Conn) {
	msg_packer := znet.NewDataPack()
	cmd_packer := server.NewCmdPack()

	for {
		cmd := parseCmd()
		// convert cmd[0] to upper case
		cmd[0] = []byte(strings.ToUpper(string(cmd[0])))
		// 退出
		if string(cmd[0]) == "EXIT" {
			break
		}
		// 检查命令是否合法, 在validCmds中
		msg_id := 0
		valid := false
		for _, v := range validCmds {
			if string(cmd[0]) == v {
				valid = true
				break
			}
		}
		if !valid {
			for _, v := range validDbCmds {
				if string(cmd[0]) == v {
					valid = true
					msg_id = 1
					break
				}
			}
		}
		if !valid {
			fmt.Printf("invalid command: %s\n", cmd[0])
			fmt.Printf("%s", prompt)
			continue
		}
		cmd_packed := cmd_packer.PackCmd(cmd)
		msg := znet.NewMessage(uint32(msg_id), cmd_packed)
		msg_packed, err := msg_packer.Pack(msg)
		if err != nil {
			panic(err.Error())
		}
		conn.Write(msg_packed)
	}

}

func parseCmd() [][]byte {
	// 将命令行参数解析为[][]byte，例如：
	// SET genshin impact
	// -> [][]byte{[]byte("SET"), []byte("genshin"), []byte("impact")}
	reader := bufio.NewReader(os.Stdin)
	line, err := reader.ReadString('\n')
	line = strings.TrimRight(line, "\n")
	if err != nil {
		panic(err.Error())
	}
	strs := strings.Split(line, " ")
	cmd := make([][]byte, len(strs))
	for i, str := range strs {
		cmd[i] = []byte(str)
	}
	return cmd
}

func main() {
	// 连接服务器
	conn, err := net.Dial("tcp", "127.0.0.1:8999")
	if err != nil {
		panic(err.Error())
	}

	go reader(conn)
	writer(conn)
}
