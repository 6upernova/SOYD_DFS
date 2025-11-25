package client

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/6upernova/SOYD_DFS/src/transport"
)

const BlockSize = 1024
const NameNodeAddr = "localhost:9000"

type DFSClient struct {
	Server *transport.Server
}

func NewDFSClient() *DFSClient {
	return &DFSClient{
		Server: transport.NewServer("Client"),
	}
}

func (c *DFSClient) Info(archiveName string) ([]transport.Label, error) {
	c.Server.MsgLog("Enviando solicitud de info sobre el archivo: " + archiveName)
	
	msg := transport.Message{
		Cmd: "INFO",
		Params: map[string]string{
			"filename": archiveName,
		},
		Metadata: nil,
		Data:     nil,
	}

	resMsg, err := c.Server.Establish_and_send(NameNodeAddr, msg)
	if err != nil {
		return nil, err
	}

	return resMsg.Metadata, nil
}

func (c *DFSClient) Put(archivePath string) error {
	data, err := os.ReadFile(archivePath)
	if err != nil {
		c.Server.MsgLog("Error al intentar leer el archivo local")
		return fmt.Errorf("error leyendo archivo: %w", err)
	}

	tempFileName := strings.Split(archivePath, "/")
	fileName := tempFileName[len(tempFileName)-1]

	var blocks [][]byte
	for i := 0; i < len(data); i += BlockSize {
		end := i + BlockSize
		if end > len(data) {
			end = len(data)
		}
		blocks = append(blocks, data[i:end])
	}

	msg := transport.Message{
		Cmd: "PUT",
		Params: map[string]string{
			"cant_blocks": strconv.Itoa(len(blocks)),
		},
		Metadata: nil,
		Data:     nil,
	}

	conn, err := c.Server.Establish_connection(NameNodeAddr)
	if err != nil {
		return fmt.Errorf("error conectando con namenode: %w", err)
	}
	
	resMsg, err := c.Server.Send_tcp_message(conn, msg)
	if err != nil {
		return fmt.Errorf("error enviando mensaje: %w", err)
	}

	c.Server.MsgLog("Enviando los bloques a los datanodes")

	fileSaved := true
	for i, m := range resMsg.Metadata {
		dnMsg := transport.Message{
			Cmd: "PUT_BLOCK",
			Params: map[string]string{
				"filename": strings.Split(fileName, ".")[0],
				"block_id": m.Block,
			},
			Data: blocks[i],
		}
		dnResMsg, err := c.Server.Establish_and_send(m.Node_address[0], dnMsg)

		if err != nil {
			c.Server.MsgLog("ERROR: al intentar establecer conexión con uno de los datanodes")
			fileSaved = false
			break
		}
		if dnResMsg.Cmd != "PUT_BLOCK_OK" {
			c.Server.MsgLog("ERROR: Uno de los bloques no pudo ser almacenado correctamente")
			fileSaved = false
			break
		}
	}

	var cmd string
	if fileSaved {
		cmd = "PUT_CONFIRMED"
		c.Server.MsgLog("SUCCESS: El archivo fue almacenado correctamente en el DFS")
	} else {
		cmd = "PUT_ERR"
		return fmt.Errorf("error almacenando archivo en el DFS")
	}

	confirmMsg := transport.Message{
		Cmd:      cmd,
		Params:   map[string]string{"filename": fileName},
		Metadata: resMsg.Metadata,
		Data:     nil,
	}
	_, _ = c.Server.Send_tcp_message(conn, confirmMsg)

	return nil
}

func (c *DFSClient) Get(archiveName string) error {
	metadata, err := c.Info(archiveName)
	if err != nil {
		return fmt.Errorf("error obteniendo info: %w", err)
	}

	var data []byte
	baseName := strings.Split(archiveName, ".")[0]

	for _, lbl := range metadata {
		var blockData []byte
		got := false

		for _, addr := range lbl.Node_address {
			msg := transport.Message{
				Cmd: "GET_BLOCK",
				Params: map[string]string{
					"filename": baseName,
					"block_id": lbl.Block,
				},
			}

			recMsg, err := c.Server.Establish_and_send(addr, msg)
			if err != nil {
				c.Server.MsgLog("ERROR: GET_BLOCK falló en " + addr + " para block " + lbl.Block)
				continue
			}

			if recMsg.Cmd == "GET_BLOCK_OK" {
				blockData = recMsg.Data
				got = true
				break
			}

			c.Server.MsgLog("ERROR: " + addr + " respondió " + recMsg.Cmd + " para block " + lbl.Block)
		}

		if !got {
			c.Server.MsgLog("ERROR FATAL: ninguna réplica respondió para el bloque " + lbl.Block)
			return fmt.Errorf("no se pudo obtener el bloque %s", lbl.Block)
		}

		data = append(data, blockData...)
	}

	return c.saveFile(data, archiveName)
}

func (c *DFSClient) Cat(filename string) (string, error) {
	metadata, err := c.Info(filename)
	if err != nil {
		return "", fmt.Errorf("error obteniendo info: %w", err)
	}
	
	var data []byte
	baseName := strings.Split(filename, ".")[0]
	
	for _, lbl := range metadata {
		var blockData []byte
		got := false
		for _, addr := range lbl.Node_address {
			msg := transport.Message{
				Cmd: "GET_BLOCK",
				Params: map[string]string{
					"filename": baseName,
					"block_id": lbl.Block,
				},
			}
			recMsg, err := c.Server.Establish_and_send(addr, msg)
			if err != nil {
				c.Server.MsgLog("ERROR: GET_BLOCK falló en " + addr + " para block " + lbl.Block)
				continue
			}
			if recMsg.Cmd == "GET_BLOCK_OK" {
				blockData = recMsg.Data
				got = true
				break
			}
			c.Server.MsgLog("ERROR: " + addr + " respondió " + recMsg.Cmd + " para block " + lbl.Block)
		}
		if !got {
			c.Server.MsgLog("ERROR FATAL: ninguna réplica respondió para el bloque " + lbl.Block)
			return "", fmt.Errorf("no se pudo obtener el bloque %s", lbl.Block)
		}
		data = append(data, blockData...)
	}
	
	return string(data), nil
}

func (c *DFSClient) saveFile(data []byte, archiveName string) error {
	path := "./local_files/" + archiveName
	tmpPath := path + ".tmp"
	err := os.WriteFile(tmpPath, data, 0644)

	if err != nil {
		c.Server.MsgLog("Error al intentar guardar el archivo: " + archiveName)
		return err
	}

	os.Rename(tmpPath, path)
	c.Server.MsgLog("Archivo: " + archiveName + " guardado en el local con éxito")
	return nil
}

func (c *DFSClient) Rm(filename string) error{
	
	msg := transport.Message{
		Cmd:"REMOVE",
		Params:map[string]string{
			"filename":filename,
		},
	}
	
	
	resMsg, err := c.Server.Establish_and_send(NameNodeAddr, msg)
	if err != nil {
		return fmt.Errorf("error enviando mensaje: %w", err)
	}

	
	fileRemoved := true
	for _, m := range resMsg.Metadata {
		dnMsg := transport.Message{
			Cmd: "RM_BLOCK",
			Params: map[string]string{
				"filename": strings.Split(filename, ".")[0],
				"block_id": m.Block,
			},
		}
		for _,addr :=range m.Node_address{

			dnResMsg, err := c.Server.Establish_and_send(addr, dnMsg)
			
			if err != nil {
				c.Server.MsgLog("ERROR: al intentar establecer conexión con uno de los datanodes")
				fileRemoved = false
				continue
			}
			if dnResMsg.Cmd != "RM_BLOCK_OK" {
				c.Server.MsgLog("ERROR: Uno de los bloques no pudo ser eliminado correctamente")
				fileRemoved = false
				continue
			}
		}

	}

	if fileRemoved {
		c.Server.MsgLog("SUCCESS: El archivo fue almacenado correctamente en el DFS")
	} else {
		c.Server.MsgLog("ATENCION: El archivo sera eliminado del indice pero quedaron nodos con el bloque")
		return fmt.Errorf("ATENCION: El archivo sera eliminado del indice pero quedaron nodos con el bloque almacenado")
	}

	return nil
}

func (c *DFSClient) Ls() ([]string, error) {
	msg := transport.Message{
		Cmd:      "LS",
		Params:   nil,
		Metadata: nil,
		Data:     nil,
	}
	
	resMsg, err := c.Server.Establish_and_send(NameNodeAddr, msg)
	if err != nil {
		return nil, fmt.Errorf("error listando archivos: %w", err)
	}

	filesStr := resMsg.Params["files"]
	if filesStr == "" {
		return []string{}, nil
	}
	files := strings.Split(filesStr, "\n")
	return files, nil
}

func (c *DFSClient) GetLocalFiles() []string {
	var files []string
	entries, err := os.ReadDir("./local_files")
	if err != nil {
		return files
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			files = append(files, entry.Name())
		}
	}
	return files
}


