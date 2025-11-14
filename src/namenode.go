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
		case "PUT": {
			cant_blocks,_ := strconv.Atoi(mensaje.Params["cant_blocks"])
			nn.put(cant_blocks,&answer_msg)
		}
	}
	
	fmt.Println(mensaje)
	err = transport.SendMessage(conn,answer_msg)	
	if err != nil {
		server.MsgLog("ERROR: al enviar el mensaje de respuesta hacia: "+ conn.RemoteAddr().String())
		return
	}
	server.MsgLog("Mensaje enviado con exito")
	
	// Solucion temporal TODO: hacerlo mas elegante
	switch mensaje.Cmd {
		case "PUT": {
			confirm_msg,_ := server.RecieveMessage(conn)
			if err != nil{
				server.MsgLog("ERROR: al recibir el mensaje desde: "+ conn.RemoteAddr().String())
				return
			}		
			nn.confirm_put(confirm_msg)
		}
	}
}

func (nn *NameNode) put(cant_blocks int, answer_msg *transport.Message){
	// implementar funcion que balancee equitativamente segun la cantidad de bloques
	// nodes := balance_charge(cant_blocks)
	var metadata []transport.Label
	for i := 0;i<cant_blocks;i++{
		metadata = append(metadata, transport.Label{Block:"b"+strconv.Itoa(i), Node_address:data_nodes[i%cant_blocks]})	
	}

	*answer_msg = transport.Message{
		Cmd:"PUT_ANSWER",
		Params:nil,
		Metadata:metadata,
		Data:nil,
	}
}

func (nn *NameNode) confirm_put(confirm_msg transport.Message ){

	if confirm_msg.Cmd != "PUT_CONFIRMED"{
		server.MsgLog("ERROR: No se ha podido confirmar el put del archivo")
		return
	}

	nn.add_metadata(confirm_msg.Params["filename"], confirm_msg.Metadata)
	nn.save_metadata()
	server.MsgLog("El PUT fue exitoso y se actualizo el indice")
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

func (nn *NameNode) add_metadata(archive_name string, nodes []transport.Label){
	nn.mu.Lock()
	nn.metadata[archive_name] = nodes
	nn.mu.Unlock()
}

func (nn *NameNode) save_metadata() error{
	nn.mu.RLock()
	data, err0 := json.MarshalIndent(nn.metadata,"", " ")		
	nn.mu.RUnlock()
	if err0 != nil{
		return err0
	}

	tmpPath := nn.path+".tmp" //Evita la corrupcion de datos si se interrumpe el proceso
	err1 := os.WriteFile(tmpPath, data, 0644)
	if err1 != nil {
		return err1
	}

	os.Rename(tmpPath, nn.path)
	
	return nil

}

