package server

import "strings"

type CmdFilePack struct {
	// read commands, don't need to persist into file
	rcmds []string
}

func NewCmdFilePack() *CmdFilePack {
	return &CmdFilePack{
		rcmds: []string{"GET", "TTL", "KEYS",
			"LLEN", "LINDEX", "LRANGE",
			"ZCARD", "ZRANGE", "ZCOUNT", "ZRANK", "ZSCORE"},
	}
}

func (this *CmdFilePack) SerializeCmd(cmd []string) string {
	for _, v := range this.rcmds {
		if cmd[0] == v {
			return ""
		}
	}

	return strings.Join(cmd, " ") + "\n"
}

func (this *CmdFilePack) UnserializeCmd(buf string) []string {
	cmd := make([]string, 0)
	word := ""
	for _, v := range buf {
		if v == ' ' || v == '\n' {
			if word != "" {
				cmd = append(cmd, word)
				word = ""
			}
			continue
		} else {
			word += string(v)
		}
	}
	return cmd
}
