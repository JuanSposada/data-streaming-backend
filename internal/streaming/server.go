package streaming

import (
	"context"
	"io"
	"log"
	"os"
	"time"

	pb "github.com/JuanSposada/data-streaming-backend/api/v1"
	"github.com/JuanSposada/data-streaming-backend/internal/cache"
)

const ChunkSize = 1024 * 1024 // 1MB por pedazo

type FileServer struct {
	pb.UnimplementedFileServiceServer
	Cache *cache.Cache
}

func (s *FileServer) StreamFile(req *pb.FileRequest, stream pb.FileService_StreamFileServer) error {
	// 1. Abrir el archivo desde el volumen de Docker
	filePath := "./uploads/" + req.FileId
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// 2. Logica de Reanudacion: Saltamos al chunk solicitado
	offset := req.StartChunk * ChunkSize
	file.Seek(offset, 0)

	buffer := make([]byte, ChunkSize)
	chunkIndex := req.StartChunk

	for {
		//Logica de control de Pausa
		// consultamos Redis antes de enviar el siguiente pedazo
		status, _ := s.Cache.GetStreamStatus(context.Background(), req.FileId)
		if status == "PAUSED" {
			log.Printf("Streaming pausado para el archivo: %s", req.FileId)
			// Guardamos el porgreso y esperamos un momeento para no saturar CPU
			s.Cache.SaveLastChunk(context.Background(), req.FileId, chunkIndex)
			time.Sleep(1 * time.Second)
			continue
		}

		n, err := file.Read(buffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		// 3. Enviar el chunk por el stream gRPC
		resp := &pb.FileResponse{
			Data:       buffer[:n],
			ChunkIndex: chunkIndex,
			IsLast:     false,
		}

		if err := stream.Send(resp); err != nil {
			return err
		}

		chunkIndex++
	}

	// 4. Avisar que terminamos
	log.Printf("Transferencia completa: %s", req.FileId)
	return stream.Send(&pb.FileResponse{IsLast: true})
}
