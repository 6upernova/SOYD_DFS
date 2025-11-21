package main

import (
	"strings"
	"time"
	"github.com/6upernova/SOYD_DFS/src/transport"
 	"net"
	"encoding/json"
	"os"
	//"bufio"
	"sync"
	//"log"
	"fmt"
	//"io"
	"strconv"
)

type DataNode struct {
	Port			string
	Path			string
	Blocks		[]BlockInfo
	mu				sync.RWMutex		
}

type BlockInfo struct {
	Filename		string
	Id					string
}

type Block []byte

var name_node_address string
var server *transport.Server

func main(){

	var port string
	if len(os.Args) != 2 {
		fmt.Println("Porfavor ingrese un numero de puerto para poder inicializar el datanode")
		return
	}
	else{
		port = Args[2]	
	}
	name_node_address = "localhost:9000"
	dn := create_data_node(port)
	dn.load_data_node()
	server = transport.NewServer("datanode:"+port+)	
	server.StartServer(":"+port, dn)
}

func (dn *DataNode) register_data_node(){

	conn := establish_connection(name_node_address)
	msg := transport.Message{
		Cmd:"REGISTER"
	}
	transport.SendMessage(conn,msg)
}

func establish_connection(node_address string) net.Conn{
	conn, err := net.Dial("tcp", node_address)
		if err != nil{
			server.MsgLog("Error al intentar establecer conexion con el namenode")
		}
		return conn
}



func (*DataNode dn) load_data_node() error {
    metaPath := dn.Path
    data, err := os.ReadFile(metaPath)
    if err != nil {
        return nil, err
    }

    var dn DataNode
    if err := json.Unmarshal(data, &dn); err != nil {
        return err
    }

    return nil
}

func 


func create_data_node(port string) *DataNode{
	return &DataNode{
		Port:port,
		Path:"./data/DNMetadata"+port+".json",
		Blocks:[]Block{}
	}	
}

func (DataNode *dn) load_blocks(){
	
}
