package main

import (
	"time"
	"github.com/6upernova/SOYD_DFS/src/transport"
 	"net"
	"encoding/json"
	"os"
	//"bufio"
	"sync"
	"log"
	"fmt"
	"io"
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

var logger *log.Logger
var data_nodes [CANT_DATANODES]string

func init() {

	init_logger()
	init_data_nodes_addrs()
	start_TCP_server()
	// Se ejecuta automáticamente antes de main()

	// Creacion del logger
	}

func init_logger(){
	f, err := os.OpenFile("./logs/namenode.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}

	logger = log.New( io.MultiWriter(os.Stdout, f), "[namenode] ", log.LstdFlags|log.Lmicroseconds,)

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


func msg_log(msg string){
	logger.Println(msg)
}

func get_local_ip() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil{
		msg_log("get_local_ip: error al establecer la conexion UDP")
	}
	defer conn.Close()

	local_address := conn.LocalAddr().(*net.UDPAddr)

	return local_address.IP.String()
}


//TCP server Managment

func (nn *NameNode) HandleConnection(conn net.Conn){


}

// Handle get request by client
// Le envia un mensaje al cliente con la informacion de los datanodes donde se encuentra el archivo
// Se queda esperando la confirmacion del cliente de que leyo exitosamente el archivo 
func (nn *NameNode ) get(archive_name string) []Label{
	
	
	msg_log("Peticion: archivo "+archive_name)
	
	nn.mu.RLock()
	data_nodes := nn.metadata[archive_name]
	nn.mu.RUnlock()
	
	if data_nodes == nil{
		println("Error archivo no existe en el sistema\n")
		msg_log("Peticion: denegada archivo no existe")
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

