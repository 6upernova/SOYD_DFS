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
	var command string
	fmt.Println("Ingrese el comando que desea ejecutar:%s")
	fmt.Scanln(&command)
	switch command{

		case "info":{
			fmt.Println("Ingrese el nombre del archivo del cual quiere saber la info")
			var archive_name string
			fmt.Scanln(&archive_name)
			info(archive_name)
		}
		case "ls":ls()
	}
}

func info(archive_name string){
	msg := transport.Message{
		Cmd:"INFO",
		Params:map[string]string{
			"filename": archive_name,
		},
		Metadata:nil,
		Data:nil,
	}

	res_msg := send_tcp_message(msg)
	
	fmt.Println(res_msg.Metadata)
}

func send_tcp_message(msg transport.Message) transport.Message{

	conn, err := net.Dial("tcp", namenode_addr)
	if err != nil{
		server.MsgLog("Error al intentar establecer conexion con el namenode")
	}


	err = transport.SendMessage(conn, msg)
	res_msg, _ := server.RecieveMessage(conn)
	server.MsgLog("Respuesta de: "+conn.RemoteAddr().String() +" recibida con exito")
	return res_msg
}


func ls(){

	msg := transport.Message{
		Cmd:"LS",
		Params:nil,
		Metadata:nil,
		Data:nil,
	}

	
	res_msg := send_tcp_message(msg)

	fmt.Println(res_msg.Params["files"])
}
