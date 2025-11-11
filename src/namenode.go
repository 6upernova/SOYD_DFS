package main

import (
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

// Informacion sobre cada bloque 
type Label struct{
	Block        string `json:"block"`
	Node_address string `json:"node"`
} 
// Representacion del namenode 
type NameNode struct {
	mu 			 sync.RWMutex
	metadata Metadata
	path 		 string
}

const(
	CANT_DATANODES = 4
)


type Metadata map[string][]Label // definicion de tipo para mayor comodidad

var data_nodes [CANT_DATANODES]string
var server *transport.Server

func init() {

	init_data_nodes_addrs()
	nn := create_name_node("./blocks/Metadata.json")
	server = transport.NewServer("namenode") //Manejado por la Api transport
	server.StartServer(":9000",nn)
	// Se ejecuta automáticamente antes de main()

	}

func init_data_nodes_addrs(){

	local_ip := get_local_ip()

	for i:=0; i< CANT_DATANODES; i++{
		data_nodes[i] = local_ip+":"+"500"+ strconv.Itoa(i)
	} 
}


func main(){
	fmt.Println(get_local_ip())
	fmt.Println(data_nodes)
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


//TCP server Managment

func (nn *NameNode) HandleConnection(conn net.Conn){
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(20*time.Second))
	server.MsgLog("Nueva conexion entrante desde:"+conn.RemoteAddr().String())

	message, err := transport.RecieveMessage(conn)
	
	if err != nil{
		server.MsgLog("Error al recibir el mensaje")
	}

	fmt.Println("El mensaje recibido es el siguiente: ")
	fmt.Println(message)

}

// Handle get request by client
// Le envia un mensaje al cliente con la informacion de los datanodes donde se encuentra el archivo
// Se queda esperando la confirmacion del cliente de que leyo exitosamente el archivo 
func (nn *NameNode ) get(archive_name string) []Label{
	
	
	server.MsgLog("Peticion: archivo "+archive_name)
	
	nn.mu.RLock()
	data_nodes := nn.metadata[archive_name]
	nn.mu.RUnlock()
	
	if data_nodes == nil{
		println("Error archivo no existe en el sistema\n")
		server.MsgLog("Peticion: denegada archivo no existe")

	}
	
	

	return nil

}






// Metadata and ED Managment
func create_name_node(metadata_path string) *NameNode{
	return &NameNode{
		metadata: make(Metadata),
		path: metadata_path,
	}
}

func (nn *NameNode) load_metadata() error{
	nn.mu.Lock()
	defer nn.mu.Unlock() //Se ejecuta al terminar la ejecucion del metodo

	data, err0 := os.ReadFile(nn.path)
	if err0 != nil{
		// Lo inicializamos vacio si el archivo no existe
		if os.IsNotExist(err0){
			nn.metadata = make(Metadata)
			return nil
		}
		return err0
	}

	var m Metadata
	err1 := json.Unmarshal(data, &m) // La libreria encoding/json se encarga de dar el formato adecuado
	if err1 != nil {
		return err1
	}
	nn.metadata = m
	return nil
}

func (nn *NameNode) save_metadata() error{
	nn.mu.RLock()
	data, err0 := json.MarshalIndent(nn.metadata,"", " ")		
	nn.mu.RUnlock()
	if err0 != nil{
		return err0
	}

	tmpPath := nn.path + ".tmp" //Evita la corrupcion de datos si se interrumpe el proceso
	err1 := os.WriteFile(tmpPath, data, 0644)
	if err1 != nil {
		return err1
	}

	os.Rename(tmpPath, nn.path)
	
	return nil

}

