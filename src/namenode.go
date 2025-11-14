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

// Representacion del namenode 
type NameNode struct {
	mu 			 sync.RWMutex
	metadata Metadata
	path 		 string
}

const(
	CANT_DATANODES = 4
)


type Metadata map[string][]transport.Label // definicion de tipo para mayor comodidad

var data_nodes [CANT_DATANODES]string
var server *transport.Server

func init() {

	init_data_nodes_addrs()
	nn := create_name_node("./blocks/Metadata.json")
	server = transport.NewServer("namenode") //Manejado por la Api transport

	nn.load_metadata()
	fmt.Println(nn.metadata)
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
	server.MsgLog("Nueva conexion entrante desde:"+ conn.RemoteAddr().String())
	
	mensaje, err := server.RecieveMessage(conn)
	if err != nil{
		server.MsgLog("ERROR: al recibir el mensaje desde: "+ conn.RemoteAddr().String())
		return
	}
	var answer_msg transport.Message

	switch mensaje.Cmd {
		case "INFO":	nn.info(mensaje.Params["filename"], &answer_msg)
		case "LS": nn.ls(&answer_msg)
		
	}
	
	fmt.Println(mensaje)
	err = transport.SendMessage(conn,answer_msg)	
	if err != nil {
		server.MsgLog("ERROR: al enviar el mensaje de respuesta hacia: "+ conn.RemoteAddr().String())
		return
	}
	server.MsgLog("Mesaje enviado con exito")
}

func (nn *NameNode) info(archive_name string, answer_msg *transport.Message){

	server.MsgLog("Peticion de info sobre: "+archive_name)
	
	nn.mu.RLock()
	archive_metadata := nn.metadata[archive_name]
	nn.mu.RUnlock()
	if archive_metadata == nil{
		server.MsgLog("El archivo: "+archive_name+" no se encuentra en el sistema")
		return
	} 

	*answer_msg = transport.Message{
		Cmd:"INFO_ANSWER",
		Params:nil,
		Metadata:archive_metadata,
		Data:nil,
	}


}

func (nn *NameNode) ls(answer_msg *transport.Message){
	
	server.MsgLog("Peticion de los archivos del sistema")

	nn.mu.RLock()
	var entries []string
	for k,_ := range nn.metadata{
		entries = append(entries, k) 
	}
	nn.mu.RUnlock()

	*answer_msg = transport.Message{
		Cmd:"LS_ANSWER",
		Params:map[string]string{
			"files":strings.Join(entries,"\n"),
		},
		Metadata:nil,
		Data:nil,
		}
}
	

// Handle get request by client
// Le envia un mensaje al cliente con la informacion de los datanodes donde se encuentra el archivo
// Se queda esperando la confirmacion del cliente de que leyo exitosamente el archivo 
func (nn *NameNode ) get(archive_name string) []transport.Label{
	
	
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
	server.MsgLog("Cargando la metadata al sistema")
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
		server.MsgLog("EROR: al leer el archivo de metadata persistente")
	}

	var m Metadata
	err1 := json.Unmarshal(data, &m) // La libreria encoding/json se encarga de dar el formato adecuado
	if err1 != nil {
		server.MsgLog("ERROR: al hacer unmarshal de metadata persistente")
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

