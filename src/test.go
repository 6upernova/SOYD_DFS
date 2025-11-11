package main


//Prueba para verificar si funciona la funcion de guardado y cargado del metadata .json
/*func main(){

	nn := create_name_node("./blocks/metadata.json")

	nn.load_metadata()

	nn.mu.Lock()
	nn.metadata["archivo1.txt"] = []Label{
		{Block: "b1", Node_address: "10.0.0.2:4000"},
	}
	nn.mu.Unlock()
	if err := nn.save_metadata(); err != nil {
		panic(err)
	}

	println("Metadata guardada correctamente.")
}
*/

/*
Codigo acoplado a namenode para levantar servidor TCP
// Revisar maneras mas robustas de hacerlo 
func start_TCP_server() error{

	listener, err := net.Listen("tcp",":9000") //TODO : agregar un struct type server para poder manejarlo con un semaforo
	if err != nil{
		msg_log("START TCP: Error opening tcp listening port")
		return err
	}

	defer listener.Close()
	msg_log("START TCP: Servidor escuchando en puerto :9000")

	for {
		conn, err := listener.Accept()
		if err != nil{
			msg_log("TCP Accept: Error al aceptar una conexion entrante ")
			continue
		}
	go handle_connection(conn) //Agregar funcion intermedia que realiza el handshke de las conexiones para mayor seguridad 
	}
}

func handle_connection(conn net.Conn){
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(20*time.Second)) // Previene inanicion
	msg_log("Nueva conexion desde"+ conn.LocalAddr().String())
	buffer := make([]byte, 1024)
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			msg_log("HANDLER: La conexion con el nodo finalizo")
			return
		}
		fmt.Printf("Recived: %s", buffer[:n])
		conn.Write([]byte("Mensaje recibido\n"))
	}
} 


*/
