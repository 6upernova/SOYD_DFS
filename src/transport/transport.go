package transport

import (
	"fmt"
 	"net"
	"encoding/json"
	"os"
	//"bufio"
	"log"
	"io"
	"encoding/binary"
	"time"
)
// Estructura del servidor inspirada en https://dev.to/jones_charles_ad50858dbc0/build-a-blazing-fast-tcp-server-in-go-a-practical-guide-29d

// Estructura de datos utilizada por el cliente y el namenode para almacenar el bloque y su localizacion
// Utilizacion de una lista de direcciones para utilizar la replicacion
type Label struct{
	Block        string `json:"block"`
	Node_address []string `json:"node"`
} 

type Server struct{
	Listener net.Listener		
	Logger	 *log.Logger
	Ip			 string
}

type Message struct{
	Cmd string 							 `json:"cmd"`
	Params map[string]string `json:"params"`
	Metadata []Label				 `json:"meta"`
	Data	[]byte 						 `json:"data"` 
}

// Declara la interfaz que deberan implementar los que deseen utilizar la api
type Handler interface {
	HandleConnection(conn net.Conn)
}


//-----------------------------------------------Public Functions----------------------------------------

func NewServer(who string) *Server{
	server := &Server{}
	server.init_logger(who)
	server.Ip = get_local_ip()
	return server
}

func (s *Server) MsgLog(msg string){
	s.Logger.Println(msg)
}



func (s *Server) StartServer(port string, handler Handler){

	var err error
	s.Listener, err = net.Listen("tcp", port)
	if err != nil{
		s.MsgLog("Error al intentar escuchar en el puerto: "+port)
		return 
	}
	defer s.Listener.Close()
	s.MsgLog("Server Started on: "+s.Ip+port)

	for {
		conn,err := s.Listener.Accept()
		if err != nil {
			s.MsgLog("Error al intentar aceptar la conexion: "+err.Error())
			continue
		}
		go handler.HandleConnection(conn)

	}
}

// Framing de mensajes (Para asegurar la integridad de los datos)

func SendMessage(conn net.Conn, msg Message) error {
	f_msg, err := json.Marshal(msg)
	if err != nil{
		return err
	}

	length := uint32(len(f_msg))
	binary.Write(conn, binary.BigEndian, length)

	_, err = conn.Write(f_msg)
	return err
}

func (s *Server) RecieveMessage(conn net.Conn) (Message, error) {
	var length uint32
	err := binary.Read(conn, binary.BigEndian, &length)

	if err != nil {
		return Message{}, err
	}

	data := make([]byte, length)
	_, err = io.ReadFull(conn, data)
	if err != nil {
		return Message{}, err
	}

	var msg Message
	err = json.Unmarshal(data, &msg)

	return msg, err
}



//-----------------------------Conexion remota---------------------------------------------------------
func (s *Server)Establish_and_send(node_address string, msg Message) (Message, error) {

    conn, err := s.Establish_connection(node_address)
    if err != nil {
        return Message{}, err
    }
    defer conn.Close()

    res_msg, err := s.Send_tcp_message(conn, msg)
    return res_msg, err
}

func (s *Server)Establish_connection(node_address string) (net.Conn, error) {
    conn, err := net.Dial("tcp", node_address)
    if err != nil {
        return nil, err
    }
    return conn, nil
}


func (s *Server) Send_tcp_message(conn net.Conn, msg Message) (Message, error) {
    if conn == nil {
        return Message{}, fmt.Errorf("conexion NULL")
    }
		conn.SetReadDeadline(time.Now().Add(20*time.Second))

    err := SendMessage(conn, msg)
    if err != nil {
        return Message{}, err
    }

    res_msg, err := s.RecieveMessage(conn)
    return res_msg, err
}



//-----------------------------Private Functions-------------------------------------------------------

func (s *Server) init_logger(who string){
	f, err := os.OpenFile("./logs/"+who+".log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}

	s.Logger = log.New( io.MultiWriter(os.Stdout, f), "["+who+"] ", log.LstdFlags|log.Lmicroseconds,)

}
func get_local_ip() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil{
		fmt.Println("Get local ip :Error al establecer conexion con servidor UDP")
	}
	defer conn.Close()

	local_address := conn.LocalAddr().(*net.UDPAddr)

	return local_address.IP.String()
}



