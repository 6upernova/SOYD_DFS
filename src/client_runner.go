package main

import (
	"fmt"
	"os"
	"github.com/6upernova/SOYD_DFS/src/client"
	"github.com/6upernova/SOYD_DFS/src/cli"
)

func main() {
	// Crear directorio local_files si no existe
	if err := os.MkdirAll("./local_files", 0755); err != nil {
		fmt.Printf("Error creando directorio local_files: %v\n", err)
		os.Exit(1)
	}

	// Inicializar cliente DFS
	dfsClient := client.NewDFSClient()
	// Ejecutar CLI
	
	if err := cli.Run(dfsClient); err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
	
}
