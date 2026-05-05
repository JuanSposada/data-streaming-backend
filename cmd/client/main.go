package main

import (
	"context"
	"io"
	"log"

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

	// 2. Pedir el archivo
	req := &pb.FileRequest{
		FileId:     "testfile10G.dat", //Nomber del archiuvo en la carpeta
		StartChunk: 0,                 // empezar desde 0
	}

	stream, err := client.StreamFile(context.Background(), req)
	if err != nil {
		log.Fatalf("Error al pedir el stream: %v", err)
	}

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

		if resp.IsLast {
			log.Println("Archivo recibido por completo!")
			break
		}

		log.Printf("Recibido Chunk #%d - Tamaño; %d bites", resp.ChunkIndex, len(resp.Data))
	}
}
