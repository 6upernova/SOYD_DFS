package main

import (
	"strings"
	"time"
	"github.com/6upernova/SOYD_DFS/src/transport"
 	"net"
	//"encoding/json"
	"os"
	//"bufio"
	//"sync"
	//"log"
	"fmt"
	//"io"
	//"strconv"
	"path/filepath"
)

type DataNode struct {
	Port			string
	Path			string
	Blocks		map[string]([]byte) //Formato key : <filename>_<blockID>
}

const name_node_address = "localhost:9000"

var server *transport.Server


func main(){

	var port string
	if len(os.Args) != 2 {
		fmt.Println("Porfavor ingrese un numero de puerto para poder inicializar el datanode")
		return
	} else{
		port = os.Args[1]	
	}
		dn := create_data_node(port)
	server = transport.NewServer("datanode:"+port)	
	dn.load_blocks()
	dn.register_data_node()
	server.StartServer(":"+port, dn)
}

func (dn *DataNode) register_data_node(){

	conn,_ := server.Establish_connection(name_node_address)

	if conn == nil{
		server.MsgLog("ERROR: No fue posible registrar el datanode por que el namenode no esta disponible")
		return
	}
	conn.SetReadDeadline(time.Now().Add(20*time.Second))
	msg := transport.Message{
		Cmd:"REGISTER",
		Params:map[string]string{
			"address":get_local_ip()+":"+dn.Port,
		},
	}
	transport.SendMessage(conn,msg)
	rec_msg,_ := server.RecieveMessage(conn)

	if rec_msg.Cmd == "REGISTER_OK"{
		server.MsgLog("SUCCESS: El datanode fue registrado con exito en el namenode")
	} else {
		server.MsgLog("ERROR: No se pudo confirmar el registro de este datanode")
	}
}

func get_local_ip() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil{
		server.MsgLog("get_local_ip: error al establecer la conexion UDP")
	}
	defer conn.Close()

	local_address := conn.LocalAddr().(*net.UDPAddr)

	return local_address.IP.String()
}




func (dn *DataNode) HandleConnection(conn net.Conn){
	defer conn.Close()

	conn.SetReadDeadline(time.Now().Add(20*time.Second))
	server.MsgLog("Nueva conexion entrante desde:"+ conn.RemoteAddr().String())
	
	mensaje, err := server.RecieveMessage(conn)
	if err != nil{
		server.MsgLog("ERROR: al recibir el mensaje desde: "+ conn.RemoteAddr().String())
		return
	}

	//Mensaje de respuesta al cliente
	var answer_msg transport.Message

	switch mensaje.Cmd {
		case "PUT_BLOCK": dn.save_block(mensaje.Params["filename"],mensaje.Params["block_id"],mensaje.Data, &answer_msg)
		case "GET_BLOCK":	dn.get_block(mensaje.Params["filename"],mensaje.Params["block_id"],&answer_msg)
		case "PING":{
			answer_msg = transport.Message{}
			server.MsgLog("El Namenode hizo PING a este datanode")
		}
		case "REPLICATE": dn.replicate(mensaje.Params["filename"], mensaje.Params["block_id"], mensaje.Params["target"], &answer_msg)
		case "RM_BLOCK": dn.rm_block(mensaje.Params["filename"], mensaje.Params["block_id"], &answer_msg)
		
	}
	
	fmt.Println(mensaje)
	err = transport.SendMessage(conn,answer_msg)	
	if err != nil {
		server.MsgLog("ERROR: al enviar el mensaje de respuesta hacia: "+ conn.RemoteAddr().String())
		return
	}
	server.MsgLog("SUCCESS: Mensaje enviado con exito")

}

func (dn *DataNode) rm_block(filename string, block_id string, answer_msg *transport.Message){
	delete(dn.Blocks,filename+"_"+block_id)
	block_path := filepath.Join(dn.Path,filename+"_"+block_id+".blk")
	err := os.Remove(block_path)
	var cmd string
	if err != nil{
		cmd = "RM_BLOCK_OK"
	}else{
		cmd = "RM_BLOCK_ERR"
	}

	*answer_msg = transport.Message{
		Cmd:cmd,
	}

}

func (dn *DataNode) replicate(filename string, block_id string, target_addr string, answer_msg *transport.Message){
	
	var cmd string
	msg_to_dest := transport.Message{
		Cmd:"PUT_BLOCK",
		Params:map[string]string{
			"filename":filename,
			"block_id":block_id,
		},
		Data:dn.Blocks[filename+"_"+block_id],
	}

	msg_to_dest_rec,_ := server.Establish_and_send(target_addr, msg_to_dest)
	if msg_to_dest_rec.Cmd == "PUT_BLOCK_OK"{
		cmd = "REPLICATE_OK"
	}else{
		cmd = "REPLICATE_ERR"
	}
	*answer_msg = transport.Message{
		Cmd:cmd,
	}
}



func (dn *DataNode) load_blocks() error {

	os.MkdirAll(dn.Path, 0755)
	entries, err := os.ReadDir(dn.Path)
	if err != nil{
		server.MsgLog("ERROR: Al intentar cargar los bloques/no hay bloques en el directorio")
		return err
	}
	
	for _, entry := range entries{
		block_path := entry.Name()
		block_no_suffix := strings.TrimSuffix(block_path,".blk")
		
		data,_ := os.ReadFile(filepath.Join(dn.Path,block_path))

		dn.Blocks[block_no_suffix] = data
	}
	
	return nil

}
func (dn *DataNode) get_block(filename string, block_id string, res_msg *transport.Message){
	
	block := dn.Blocks[filename+"_"+block_id]
	var cmd string
	if block == nil{
		server.MsgLog("ERROR: El bloque solicitado no se encuentra en este datanode")
		cmd = "GET_BLOCK_ERR"
	} else{
		cmd = "GET_BLOCK_OK"
	}

	*res_msg = transport.Message{
		Cmd:cmd,
		Data:block,
	}
} 
func (dn *DataNode) save_block(filename string, block_id string, data []byte, res_msg *transport.Message){
	block_path := filepath.Join(dn.Path,filename+"_"+block_id+".blk")
	dn.Blocks[filename+"_"+block_id] = data

	err := os.WriteFile(block_path,data,0644)

	var cmd string
	if err != nil{
		server.MsgLog("ERROR: al intentar guardar el bloque: "+filename+"_"+block_id)
		cmd = "PUT_BLOCK_ERR"
	}else{
		server.MsgLog("SUCCESS: bloque "+filename+"_"+block_id+"almacenado con exito")
		cmd = "PUT_BLOCK_OK"
	}
	*res_msg = transport.Message{
		Cmd:cmd,
	}

}



func create_data_node(port string) *DataNode{
	return &DataNode{
		Port:port,
		Path:"./blocks/"+port+"/",
		Blocks:make(map[string]([]byte)),
	}	
}


