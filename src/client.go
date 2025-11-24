package main

import (
	"fmt"
	"github.com/6upernova/SOYD_DFS/src/transport"
 	//"net"
	"strconv"
	"strings"
	//"bufio"
	//"log"
	//"io"
	"os"
)

const BlockSize = 1024
var namenode_addr string
var server *transport.Server



func main(){

	namenode_addr = "localhost:9000"	
	server = transport.NewServer("Client")
	var command string

	for{
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
			case "q":return
			case "get":{
				fmt.Println("Ingrese el nombre del archivo que desea obtener")
				var archive_name string
				fmt.Scanln(&archive_name)
				get(archive_name)

			}
			case "put":{
				fmt.Println("Ingrese la ruta del archivo que desea colocar en el sistema")
				var archive_path string
				fmt.Scanln(&archive_path)
				put(archive_path)

			} 
		}
	}
}

func info(archive_name string) []transport.Label{
	server.MsgLog("Eviando solicitud de info sobre el archivo: "+archive_name)
	msg := transport.Message{
		Cmd:"INFO",
		Params:map[string]string{
			"filename": archive_name,
		},
		Metadata:nil,
		Data:nil,
	}

	res_msg,_ := server.Establish_and_send(namenode_addr, msg)
	
	fmt.Println(res_msg.Metadata)

	return res_msg.Metadata
}

func put(archive_path string){

	data, err := os.ReadFile(archive_path)
	if err != nil{
		server.MsgLog("Error al intentar leer el archivo local")
		return
	}

	temp_file_name := strings.Split(archive_path, "/")
	file_name := temp_file_name[len(temp_file_name)-1]

	fmt.Println(file_name)

	var blocks [][]byte
	for i := 0; i < len(data); i += BlockSize {
    end := i + BlockSize
    if end > len(data) { end = len(data) }
    blocks = append(blocks, data[i:end])
	}

	
	msg:= transport.Message{
		Cmd:"PUT",
		Params: map[string]string{
			"cant_blocks":strconv.Itoa(len(blocks)),
		},
		Metadata:nil,
		Data:nil,
	}	

	conn,_ := server.Establish_connection(namenode_addr)
	res_msg,_ :=server.Send_tcp_message(conn, msg)
	
	server.MsgLog("Enviando los bloques a los datanodes")
	
	file_saved := true
	for i,m := range res_msg.Metadata{
		dn_msg := transport.Message{
			Cmd:"PUT_BLOCK",
			Params:map[string]string{
				"filename":strings.Split(file_name,".")[0],
				"block_id":m.Block,
			},
			Data:blocks[i],
		}
		dn_res_msg,err := server.Establish_and_send(m.Node_address[0],dn_msg)

		if err != nil{
			server.MsgLog("ERROR: al intentar establecer conexion con uno de los datanodes y por ende almacenar el archivo")
			file_saved = false
			break
		}
		if dn_res_msg.Cmd != "PUT_BLOCK_OK"{
			server.MsgLog("ERROR: Uno de los bloques no pudo ser almacenado correctamente")
			file_saved = false
			break
		}
		
	}
	var cmd string
	if file_saved{
		cmd = "PUT_CONFIRMED"
		server.MsgLog("SUCCESS: El archivo fue almacenado correctamente en el DFS")
	}else{
		cmd = "PUT_ERR"
	}
	confirm_msg := transport.Message{
		Cmd:cmd,
		Params:map[string]string{
			"filename":file_name,
		},
		Metadata:res_msg.Metadata,
		Data:nil,
	}
	_,_ = server.Send_tcp_message(conn,confirm_msg)
}

func get(archive_name string) {
	metadata := info(archive_name)
	var data []byte

	baseName := strings.Split(archive_name, ".")[0]

	for _, lbl := range metadata {
		var blockData []byte
		got := false

		// probar todas las réplicas del bloque hasta lograr obtenerlo
		for _, addr := range lbl.Node_address {

			msg := transport.Message{
					Cmd: "GET_BLOCK",
					Params: map[string]string{
							"filename": baseName,
							"block_id": lbl.Block,
					},
			}

			recMsg, err := server.Establish_and_send(addr, msg)
			if err != nil {
					server.MsgLog("ERROR: GET_BLOCK falló en " + addr + " para block " + lbl.Block)
					continue
			}

			if recMsg.Cmd == "GET_BLOCK_OK" {
					blockData = recMsg.Data
					got = true
					break
			}

			server.MsgLog("ERROR: " + addr + " respondió " + recMsg.Cmd + " para block " + lbl.Block)
		}

		if !got {
			server.MsgLog("ERROR FATAL: ninguna réplica respondió para el bloque " + lbl.Block)
			return
		}

		data = append(data, blockData...)
	}

	save_file(data, archive_name)
}

func save_file(data []byte, archive_name string) error{

	path := "./local_files/"+archive_name
	tmp_path := path+".tmp" 
	err := os.WriteFile(tmp_path, data, 0644)

	if err != nil{
		server.MsgLog("Error al intentar guardar el archivo: "+archive_name)
		return err
	}

	os.Rename(tmp_path, path)
	
	server.MsgLog("Archivo: "+archive_name+" guardado en el local con exito")
	return nil

}

func ls(){

	msg := transport.Message{
		Cmd:"LS",
		Params:nil,
		Metadata:nil,
		Data:nil,
	}

	
	res_msg,_ := server.Establish_and_send(namenode_addr, msg)

	fmt.Println(res_msg.Params["files"])
}



  

