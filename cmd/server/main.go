package main

import (
	"log"
	"net"

	pb "github.com/JuanSposada/data-streaming-backend/api/v1"
	"github.com/JuanSposada/data-streaming-backend/internal/cache"
	"github.com/JuanSposada/data-streaming-backend/internal/streaming"
	"google.golang.org/grpc"
)

func main() {
	// 1. Inicializar Redis Usando direccion del contenedor de Docker
	//Para pruebasds locales sin docker se usa "localhost:6379"
	redisCache := cache.NewCache("redis:6379")
	log.Println("Conectando a Redis...")

	// 2. Configurar el listener TCP para gRPC
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Error al abrir el puerto 50051: %v", err)
	}

	// 3. Crear el servidor gRPC y registrar nuestro servicio
	s := grpc.NewServer()

	// Pasamos el cache al servidor de streaming
	fileServer := &streaming.FileServer{
		Cache: redisCache,
	}

	pb.RegisterFileServiceServer(s, fileServer)
	log.Println("Servidor de Streaming escuchando en :50051...")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Error al servir gRPC: %v", err)
	}
}
