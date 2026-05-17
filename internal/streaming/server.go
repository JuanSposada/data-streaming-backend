package streaming

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	pb "github.com/JuanSposada/data-streaming-backend/api/v1"
	"github.com/JuanSposada/data-streaming-backend/internal/cache"
	"github.com/nats-io/nats.go"
)

const ChunkSize int64 = 1024 * 1024 // 1MB por pedazo

type FileServer struct {
	pb.UnimplementedFileServiceServer
	Cache *cache.Cache
	Nats  *nats.Conn
}

func (s *FileServer) StreamFile(req *pb.FileRequest, stream pb.FileService_StreamFileServer) error {
	// 0.5 agregamos el evento de Nats al inicion
	s.Nats.Publish("file.status", []byte("Iniciado: "+req.FileId))

	// 1. Abrir el archivo desde el volumen de Docker
	filePath := "./uploads/" + req.FileId
	file, err := os.Open(filePath)
	fileInfo, err := file.Stat()
	if err != nil {
		return err
	}

	totalSize := fileInfo.Size()

	totalChunks := totalSize / ChunkSize

	if totalSize%ChunkSize != 0 {
		totalChunks++
	}

	defer file.Close()

	// 2. Logica de Reanudacion: Saltamos al chunk solicitado
	offset := int64(req.StartChunk) * ChunkSize
	log.Printf("reanindando desde byte: %d (Chunk: %d)", offset, req.StartChunk)

	_, err = file.Seek(offset, 0)
	if err != nil {
		return err
	}

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
			Data:        buffer[:n],
			ChunkIndex:  chunkIndex,
			TotalChunks: totalChunks,
			IsLast:      false,
		}

		if err := stream.Send(resp); err != nil {
			//Evento de NATS
			s.Nats.Publish("file.errors", []byte("DISCONNECT: Conexion perdida con cliente en "+req.FileId))
			return err
		}

		//Notificar progreso
		if chunkIndex%10 == 0 {
			s.Nats.Publish("file.progress", []byte(fmt.Sprintf("Archivo %s en chunk %d", req.FileId, chunkIndex)))
		}
		chunkIndex++
	}

	// 4. Avisar que terminamos
	s.Nats.Publish("file.status", []byte("COMPLETADO: "+req.FileId))
	log.Printf("Transferencia completa: %s", req.FileId)
	return stream.Send(&pb.FileResponse{IsLast: true})
}

func (s *FileServer) UploadFile(stream pb.FileService_UploadFileServer) error {
	var file *os.File
	var fileName string
	// ⚠️ CONFIGURACIÓN CRÍTICA: Cambia este número al tamaño exacto de tus chunks en el Frontend.
	// Si en tu JS usas bloques de 1MB, deja 1024 * 1024. Si usas 5MB, multiplica por 5.
	const ChunkSize = 1024 * 1024

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			if file != nil {
				file.Close()
			}
			return stream.SendAndClose(&pb.UploadResponse{Message: "Chunks posicionados con éxito", Success: true})
		}
		if err != nil {
			if file != nil {
				file.Close()
			}
			return err
		}

		fileName = req.GetFileName()
		path := "./uploads/" + fileName

		// Abrimos el archivo en modo lectura/escritura común. Si no existe, lo crea.
		file, err = os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Printf("Error abriendo archivo en Go: %v", err)
			return err
		}

		// Control de Pausa por Redis
		for {
			status, _ := s.Cache.GetStreamStatus(context.Background(), fileName)
			if status != "PAUSED" {
				break
			}
			time.Sleep(1 * time.Second)
		}

		// 🧮 LA MAGIA: Calculamos el offset exacto en el disco usando el chunk_index
		offset := int64(req.GetChunkIndex()) * ChunkSize

		// Escribimos los bytes en su lugar absoluto, ignorando el orden en que llegaron
		_, err = file.WriteAt(req.GetData(), offset)
		if err != nil {
			log.Printf("Error escribiendo en offset %d: %v", offset, err)
			file.Close()
			return err
		}

		file.Close() // Cerramos el descriptor inmediatamente para liberar el archivo
		s.Nats.Publish("file.progress", []byte(fmt.Sprintf("Chunk %d guardado en su lugar", req.GetChunkIndex())))
	}
}
