
# Estructura general del proyecto

    ├── cli
    │   └── client_cli.go --> Front end de la aplicacion del cliente(Pura facha)
    ├── client
    │   └── client.go --> Logica de la parte del cliente
    ├── client_runner.go --> Runner que unifica la cli con la logica
    ├── datanode.go --> Donde se almacenan los bloques
    ├── local_files --> Se almacenan los archivos locales de cada cliente
    ├── namenode.go --> Gestor de metadata e indicie del servidor
    └── transport
        └── transport.go --> Interfaz comun que se encarga de encapsular el envio y recepcion de mensajes por TCP
## IMPORTANTE

Si se quiere ejecutar el proyecto se hace desde la carpeta SOYD_DFS y se ejecuta:
`$ go run ./src/namenode.go`

Es por un tema de los paths relativos y absolutos
# Funcionalidades Basicas
- Put
- Get
- Ls
- Info
- Loggers
# Extras

- Framing de mensajes
- Ping a datanodes para saber si estan levantados: Es una go routine aparte del loop principal que esta constantemente preguntando a los nodos up si siguen activos,
- Registro persistente de datanodes al sisema
	- Si se cae alguno o se cae el namenode, este se vuelve a registrar con todos los datos que ya poseia
- Replicacion automatica dado un factor de replicacion: El namenode le ordena al nodo que tiene el bloque que le envie a otro nodo disponible el bloque
- Balanceo de carga: Se actualiza constantemente en base a la cant de bloques de cada datanode
- Cat y Remove
- CLI Amigable 
	- Display de archivos remotos y locales como extra

## Detalles de implementacion

### Namenode

```Go
type NameNode struct {
	mu 			 sync.RWMutex
	metadata Metadata
	path 		 string
}
type DataNode struct {
	Address					string `json:"ip"`
	Cant_blocks		        int	   `json:"cant_blocks"`
}
const(
	REPLICATION_FACTOR = 2
	CANT_DATANODES = 4
)
type Metadata map[string][]transport.Label 
var data_nodes []DataNode
var data_nodes_up []DataNode
var dns_mu sync.RWMutex

var server *transport.Server

```

Se utiliza la estrucutra Namenode para representar al mismo, todos los metodos deberan ser invocados atravez de la unica instancia de esta estructura(Se crea y se incializa al comenzar el programa cargandole asi toda la metadata persistente).

Luego la otra estructura es utilizada en 2 listas data_nodes y data_nodes_up que son utilizados para saber que cantidad de bloques tiene cada nodo y cuales de ellos estan levantados, Estas listas son actualizadas cuando se modifica algun archivo del sistema.

data_nodes  es una lista persistente (Se guarda en datanodes.json)
mientras que la otra es dinamica y de TJ --> se actualiza en base al ping y a la otra lista
Son utilizadas para hacer el balanceo de carga --> Siempre estan ordenadas desde el que tiene menos bloques al que mas
Para determinar que nodos estan disponibles actualmente y no enviarle al cliente informacion erronea.

Aclaracion: transport.Label es de esta forma(Declarado en la interfaz de transport) :
```Go
type Label struct{
	Block        string `json:"block"`
	Node_address []string `json:"node"`
} 
```

### Transport
```Go
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

func NewServer(who string) *Server{
	server := &Server{}
	server.init_logger(who)
	server.Ip = get_local_ip()
	return server
}


```

Esta api la utilizan todos los programas del proyecto, ya que permite crear e inicializar el servidor de escucha asi como el main loop para manejar las conexiones entrantes.
Ademas declara e inicializa el loger personal dependiendo quien lo este utilizando el cual es luego utilizado para guardar en local todas las operaciones(tambien por pantalla)
Tambien declara el formato de mensaje utilizado a lo largo de la aplicacion, asi como el framing de mensajes haciendo asi que solo se puedan enviar mensajes con determinado formato y no se gaste ancho de banda.
### Datanode

```Go
type DataNode struct {
	Port			string
	Path			string
	Blocks		map[string]([]byte) //Formato key : <filename>_<blockID>
}

const name_node_address = "localhost:9000"

var server *transport.Server
```
Almacena los bloques fisicamente en el local y levanta un servidor de escucha donde recibe instrucciones sobre que hacer con dichos bloques(desde el cliente), Tambien recibe la instruccion ping del namenode para saber si esta vivo
Asi como de otros datanodes enviandole una replica de algun bloque(Manejado desde el namenode).
### Cliente

El encargado de enviar las instrucciones al namenode como a los datanodes indicados(si es el caso put, get, rm), no tiene mayor misterio las operaciones son claras y concisas gracias al encapsulamiento de la logica de conexion que provee transport. Algunas operaciones estan adaptadas para que devuelvan un output que  la cli pueda manejar y displayear
### Cliente_Cli

Interfaz de la cli para que sea mas facil ejecutar las operaciones del servidor, Esta hecho utilizando la libreria https://github.com/charmbracelet/lipgloss, Se lo pedi a chatgpt, este me devolvio un esqueleto razonable y yo realizae los arreglos necesarios asi como la integracion con el cliente (no fue tarea facil).