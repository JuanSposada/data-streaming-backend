package main

import (
	"context"
	"io"
	"log"
	"os"
	"time"

	pb "github.com/JuanSposada/data-streaming-backend/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// 1. Conectar al servidor (localhos porque este es simulador del cliente que esta fuera del contenedor)
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("No pude conectar: %v", err)
	}
	defer conn.Close()

	client := pb.NewFileServiceClient(conn)

	// TEST de Subida
	testFileToUpload := "test_tesis.txt"
	// Creamos nun archivo rapido para la prueba si no existe
	os.WriteFile(testFileToUpload, []byte("Contenido de prueba para el sistema de streaming gRPC"), 0644)
	log.Println("--Iniciando Test de Subida ---")
	err = uploadTest(client, testFileToUpload)
	if err != nil {
		log.Printf("Fallo la subida: %v", err)
	}

	//Paso 2: Test de Descarga
	log.Println("\n---- Iniicando Test de Descarga ----")
	downloadFileName := "v1.mp4"
	downloadTest(client, downloadFileName)

}

// Funcion para subir Archivos (client-side Streaming)
func uploadTest(client pb.FileServiceClient, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	stream, err := client.UploadFile(context.Background())
	if err != nil {
		return err
	}

	buffer := make([]byte, 512*1024)
	for {
		n, err := file.Read(buffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		err = stream.Send(&pb.UploadRequest{
			FileName: filePath,
			Data:     buffer[:n],
		})
		if err != nil {
			return err
		}
	}

	res, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}
	log.Printf("Resultado subida: %s (Exito: %v)", res.Message, res.Success)
	return nil
}

func downloadTest(client pb.FileServiceClient, fileName string) {
	req := &pb.FileRequest{
		FileId:     fileName, //Nomber del archiuvo en la carpeta
		StartChunk: 0,        // empezar desde 0
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	stream, err := client.StreamFile(ctx, req)
	if err != nil {
		log.Fatalf("Error al pedir el stream: %v", err)
	}

	outputFile, err := os.Create("descargado_" + fileName)
	if err != nil {
		log.Fatalf("No se pudo crear archivo local: %v", err)
	}
	defer outputFile.Close()

	log.Println("Empezando a recibir chunks...")

	// 3. Recibir los chunks
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Error recibiendo: %v", err)
		}

		_, err = outputFile.Write(resp.Data)
		if err != nil {
			log.Fatalf("Error al escribir en disco: %v", err)
		}

		log.Printf("Recibido Chunk #%d - Tamaño; %d bites", resp.ChunkIndex, len(resp.Data))

		if resp.IsLast {
			log.Println("Archivo recibido por completo!")
			break
		}

	}
	log.Printf("El archivo se guardo como: descargado_%s", fileName)
}
