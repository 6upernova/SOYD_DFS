package main

import (
	"fmt"
	"github.com/6upernova/SOYD_DFS/src/transport"
 	"net"
	//"bufio"
	//"log"
	//"io"
)

var namenode_addr string
var server *transport.Server

func main(){
	namenode_addr = "localhost:9000"	
	server = transport.NewServer("Client")
	info()
	
}

func info(){
	mensaje := transport.Message{
		Cmd:"INFO",
		Params:map[string]string{
		"filename": "archivo.txt",
	},
		Data:nil,
	}
	
	conn, err := net.Dial("tcp", namenode_addr)
	if err != nil{
		panic(err)
	}

	err = transport.SendMessage(conn, mensaje)
	mensaje_rec, _ := server.RecieveMessage(conn)
	fmt.Println(mensaje_rec)


}


