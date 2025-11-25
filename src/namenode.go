package main

import (
	//"container/heap"
	"slices"
	"sort"
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



type DataNode struct {
	Address					string `json:"ip"`
	Cant_blocks			int		 `json:"cant_blocks"`
}

const(
	REPLICATION_FACTOR = 2
	CANT_DATANODES = 4
)


type Metadata map[string][]transport.Label // definicion de tipo para mayor comodidad

// Lo utilizamos para autenticacion basica de data nodes y balanceo de carga 

var data_nodes []DataNode
var data_nodes_up []DataNode
var dns_mu sync.RWMutex

var server *transport.Server

func main(){
	nn := create_name_node("./data/metadata.json")
	server = transport.NewServer("namenode") //Manejado por la Api transport

	nn.load_metadata()
	ping_data_nodes_before_start()
	go nn.start_replication_service()
	fmt.Println(nn.metadata)
	fmt.Println(data_nodes)
	server.StartServer(":9000",nn)
	// Se ejecuta automáticamente antes de main()


}

// Este servicio se encarga de generar replicas para cada uno de los bloques almacenados en la metadata
// Utiliza la lista de data_nodes_up para saber si los nodos que contienen las replicas siguen despiertos
func (nn *NameNode) start_replication_service(){

	for {
		ping_alive_data_nodes()
		//Solamente arranca el servicio de replicacion si hay mas de 2 DN conectados
		if len(data_nodes_up) > 2{
			nn.check_replication()
		}
		time.Sleep(2*time.Minute)
	}
}

func (nn *NameNode) check_replication(){
	
	for filename, labels := range nn.metadata{
		nn.mu.RLock()
		block_map := build_blocks_map(labels)
		nn.mu.RUnlock()	
		for blockID,replics_addr := range block_map{
			if (len(replics_addr) < REPLICATION_FACTOR){
				nn.replicate_block(filename,blockID,replics_addr)
			}
		//TODO: Manejar si un nodo esta caido y hacer una replica 
		}

	}
}


func (nn *NameNode) replicate_block(filename string, block_id string, replics_addr []string){
	
	if len(replics_addr) == 0{
		return
	}

	origin := replics_addr[0]
	dest := choose_destination(origin)

	msg := transport.Message{
		Cmd:"REPLICATE",
		Params:map[string]string{
			"filename": strings.Split(filename,".")[0],
			"block_id":block_id,
			"target": dest,
		},
	}


	rec_msg, err := server.Establish_and_send(origin, msg)
	if err != nil{
		server.MsgLog("Error al intentar crear la replica del bloque: "+block_id)
		return
	}	

	if rec_msg.Cmd == "REPLICATE_OK"{
		nn.add_replica(filename,block_id,dest)
		nn.save_metadata()
	}
	

}

func choose_destination(origin_node string)(dest_addrr string){
	
	var return_addr string
	//como estan balanceados siempre va a devolver al mejor nodo que no sea el de origen
	for _,node := range data_nodes_up{
		if node.Address != origin_node{
			return_addr = node.Address
			break
		}
	}


	return return_addr

}

//Solucion temporal es mas eficiente tener una variable global y actualizarla cuando se modifique la metadata
func build_blocks_map(labels []transport.Label) map[string][]string {
    m := make(map[string][]string)
    for _, lbl := range labels {
        m[lbl.Block] = append(m[lbl.Block], lbl.Node_address...)
    }
    return m
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

	//Mensaje de respuesta al cliente
	var answer_msg transport.Message

	switch mensaje.Cmd {
		case "REGISTER": register_data_node(mensaje.Params["address"], &answer_msg) 
		case "INFO":	nn.info(mensaje.Params["filename"], &answer_msg)
		case "LS": nn.ls(&answer_msg)
		case "PUT": {
			cant_blocks,_ := strconv.Atoi(mensaje.Params["cant_blocks"])
			nn.put(cant_blocks,&answer_msg)
		}
		case "REMOVE": nn.rm(mensaje.Params["filename"],&answer_msg)
	}
	
	fmt.Println(mensaje)
	err = transport.SendMessage(conn,answer_msg)	
	if err != nil {
		server.MsgLog("ERROR: al enviar el mensaje de respuesta hacia: "+ conn.RemoteAddr().String())
		
		return
	}
	server.MsgLog("SUCCESS: Mensaje enviado con exito")
	
	// En caso de necesitar volver a enviar una respuesta
	switch mensaje.Cmd {
		case "PUT": {
			confirm_msg,err := server.RecieveMessage(conn)
			if err != nil{
				server.MsgLog("ERROR: al recibir el mensaje desde: "+ conn.RemoteAddr().String())
				return
			}		
			nn.confirm_put(confirm_msg)
		}
	}
}

func register_data_node(node_address string, res_msg *transport.Message){

	var nodesAddrSet []string
	node_pos := -1
	for i, n := range data_nodes {
		nodesAddrSet = append(nodesAddrSet,n.Address)
		if n.Address == node_address{
			node_pos = i
		}
	}
	var nodesUpAddrSet []string
	for _,n := range data_nodes_up {
		nodesUpAddrSet = append(nodesUpAddrSet, n.Address)
	}
	if !slices.Contains(nodesAddrSet, node_address){	
		node:=DataNode{
			Address:node_address,
			Cant_blocks:0,
		}	
		//Si no existe lo agregamos a ambas listas
		data_nodes =append(data_nodes, node)
		data_nodes_up = append(data_nodes_up, node)
	}else if !slices.Contains(nodesUpAddrSet, node_address){
		//Si existe lo recuperamos de la memoria persistente y lo colocamos en los que estan levantados
		if node_pos != -1{
			data_nodes_up = append(data_nodes_up,data_nodes[node_pos])
		} 
	}					


	*res_msg = transport.Message{
		Cmd:"REGISTER_OK",
	}

	balance_charge()
	fmt.Println(data_nodes)
	fmt.Println(data_nodes_up)
}

func PING(node_address string) bool{
	if node_address == "" {
		server.MsgLog("PING recibido con dirección vacía")
		return false
	}

	msg := transport.Message{
		Cmd: "PING",	}

	_, err := server.Establish_and_send(node_address, msg)
	if err != nil {
		return false
	}
	return true
}

// Esta funcion checkea todos los nodos de la lista persistente de datanodos y actualiza la de TE para saber cuales estan disponibles 
func ping_data_nodes_before_start(){
	for _,node := range data_nodes{
		if PING(node.Address){
			data_nodes_up = append(data_nodes_up,node)
		}
	}
	balance_charge()
}

func ping_alive_data_nodes(){
	
	dns_mu.Lock()
	for i := 0; i < len(data_nodes_up); {
		if !PING(data_nodes_up[i].Address) {
			//Magia negra para borrar elementos de un slice
			data_nodes_up = append(data_nodes_up[:i], data_nodes_up[i+1:]...)
			continue  		
		}
		i++
	}
	dns_mu.Unlock()
	balance_charge()

	fmt.Println("CURRENTLY UP DATANODES: ")
	fmt.Println(data_nodes_up)

}


func (nn *NameNode) put(cant_blocks int, answer_msg *transport.Message){
	// La funcion de balanceo de carga esta implementada de manera que se mantiene una lista actualizada en tiempo real de los nodos menos cargados
	var metadata []transport.Label
	for i := 0; i < cant_blocks; i++ {
		dn := data_nodes_up[i%len(data_nodes_up)]
		metadata = append(metadata, transport.Label{
				Block:        "b" + strconv.Itoa(i),
				Node_address: []string{dn.Address, },
		})
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
	// Los datandes siempre estan almacenados de menor carga a mayor carga
	balance_charge()
	nn.save_metadata()
	server.MsgLog("SUCCESS: El PUT fue exitoso y se actualizo el indice")
}

func (nn *NameNode) info(archive_name string, answer_msg *transport.Message){

	server.MsgLog("Peticion de info sobre: "+archive_name)
	
	nn.mu.RLock()
	archive_metadata := nn.metadata[archive_name]
	nn.mu.RUnlock()
	if archive_metadata == nil{
		server.MsgLog("ERROR: El archivo: "+archive_name+" no se encuentra en el sistema")
		return
	} 

	*answer_msg = transport.Message{
		Cmd:"INFO_ANSWER",
		Params:nil,
		Metadata:archive_metadata,
		Data:nil,
	}


}

func balance_charge() {

	dns_mu.Lock()
	defer dns_mu.Unlock()
	sort.Slice(data_nodes, func(i, j int) bool{
		return data_nodes[i].Cant_blocks < data_nodes[j].Cant_blocks
	} )

	sort.Slice(data_nodes_up, func(i, j int) bool{
		return data_nodes_up[i].Cant_blocks < data_nodes_up[j].Cant_blocks
	} )
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
	
func (nn *NameNode) rm(filename string, answer_msg *transport.Message){

	nn.info(filename,answer_msg)
	

	metadata := answer_msg.Metadata
	nn.remove_metadata(filename)
	*answer_msg = transport.Message{
		Cmd:"RM_ANSWER",
		Metadata:metadata,
	}

	nn.save_metadata()
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


	dn_path := strings.Replace(nn.path, "metadata.json", "datanodes.json", 1)

	data, err0 := os.ReadFile(nn.path)
	if err0 != nil{
		// Lo inicializamos vacio si el archivo no existe
		if os.IsNotExist(err0){
			nn.metadata = make(Metadata)
			load_nodes_metadata(dn_path)
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

	
	err :=	load_nodes_metadata(dn_path)
	if (err != nil){
		server.MsgLog("ERROR: Al intentar cargar la metadata de los datanodes")
		return err
	}
	return nil
}



func (nn *NameNode) add_metadata(archive_name string, nodes []transport.Label) {
	nn.mu.Lock()
	defer nn.mu.Unlock()

	nn.metadata[archive_name] = nodes

	// indexar data_nodes por address
	index := make(map[string]int)
	for i, dn := range data_nodes {
			index[dn.Address] = i
	}

	index2 := make(map[string]int)
	for i, dn := range data_nodes_up{
		index2[dn.Address] = i
	}

	// iterar todas las direcciones de cada label
	for _, lbl := range nodes {
			for _, addr := range lbl.Node_address {
					if pos, ok := index[addr]; ok {
							data_nodes[pos].Cant_blocks++
					}
					if pos, ok := index2[addr]; ok{
						data_nodes_up[pos].Cant_blocks++
					}
			}
	}
}
func (nn *NameNode) remove_metadata(archive_name string) {
	nn.mu.Lock()
	defer nn.mu.Unlock()

	nodes, exists := nn.metadata[archive_name]
	if !exists {
		return
	}

	index := make(map[string]int)
	for i, dn := range data_nodes {
		index[dn.Address] = i
	}

	index2 := make(map[string]int)
	for i, dn := range data_nodes_up {
		index2[dn.Address] = i
	}

	for _, lbl := range nodes {
		for _, addr := range lbl.Node_address {

			if pos, ok := index[addr]; ok {
				if data_nodes[pos].Cant_blocks > 0 {
					data_nodes[pos].Cant_blocks--
				}
			}

			if pos, ok := index2[addr]; ok {
				if data_nodes_up[pos].Cant_blocks > 0 {
					data_nodes_up[pos].Cant_blocks--
				}
			}
		}
	}

	delete(nn.metadata, archive_name)
	server.MsgLog("Metadata Eliminada con exito")
}

func (nn *NameNode) add_replica(filename, blockID, nodeAddr string) {
	nn.mu.Lock()

	labels := nn.metadata[filename]
	for _, dn := range data_nodes {
		if dn.Address == nodeAddr{
			dn.Cant_blocks++
		}
	}

	for _, dn := range data_nodes_up{
		if dn.Address == nodeAddr{
			dn.Cant_blocks++
		}
	}

	for i := range labels {
			lbl := &labels[i]
			if lbl.Block == blockID {

					for _, addr := range lbl.Node_address {
							if addr == nodeAddr {
									return // ya existe
							}
					}

					lbl.Node_address = append(lbl.Node_address, nodeAddr)

					nn.metadata[filename] = labels
					nn.mu.Unlock()
					return
			}
	}
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
	
	dn_path := strings.Replace(nn.path, "metadata.json", "datanodes.json", 1)
	err := save_nodes_metadata(dn_path )
	if (err != nil){
		server.MsgLog("ERROR: Al intentar guardar la metadata de los datanodes")
		return err
	}
	server.MsgLog("SUCCES: Se guardo exitosamente la metadata en metadata.json")
	return nil

}

//Data nodes persisten information Managment

func load_nodes_metadata(path string) error{
	data, err0 := os.ReadFile(path)
	if err0 != nil{
		// Lo inicializamos vacio si el archivo no existe
		if os.IsNotExist(err0){
			data_nodes = []DataNode{}
			return nil
		}
		return err0
		server.MsgLog("ERROR: al leer el archivo de datanodes persistente")
	}

	var m []DataNode
	err1 := json.Unmarshal(data, &m) // La libreria encoding/json se encarga de dar el formato adecuado
	if err1 != nil {
		server.MsgLog("ERROR: al hacer unmarshal de metadata persistente")
		return err1
	}
	data_nodes=m
	return nil
}

func save_nodes_metadata(path string) error{
	
	data, err0 := json.MarshalIndent(data_nodes,"", " ")

	if err0 != nil{
		return err0
	}

	tmpPath := path+".tmp" //Evita la corrupcion de datos si se interrumpe el proceso
	err1 := os.WriteFile(tmpPath, data, 0644)
	if err1 != nil {
		return err1
	}

	os.Rename(tmpPath,path) 
	
	return nil	
}


