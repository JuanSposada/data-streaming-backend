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

const ChunkSize = 1024 * 1024 // 1MB por pedazo

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
	var totalReceived int64

	for {
		// 1. recibimos el siguiente mensaje del flujo de stream
		req, err := stream.Recv()

		// si err es io.EOF significa que el cliente ya envio todo
		if err == io.EOF {
			log.Printf("Subida finalizata. Total: %d bytes", totalReceived)

			//Notificamos a NATS el exito
			s.Nats.Publish("file.status", []byte("COMPLETADO: el archivo "+fileName+"se subio correctamente"))

			return stream.SendAndClose(&pb.UploadResponse{
				Message: "!Archivo recibido y guardado con exito!",
				Success: true,
			})
		}
		if err != nil {
			log.Printf("Error recibiendo el stream: %v", err)
			return err
		}

		// 2. Si es el primer chunk, preparamos el archivo en disco
		if file == nil {
			fileName = req.GetFileName()
			path := "./uploads/" + fileName

			log.Printf("Iniciando subida de: %s", fileName)
			s.Nats.Publish("file.status", []byte("SUBIENDO: "+fileName))

			file, err = os.Create(path)
			if err != nil {
				return err
			}
			defer file.Close()
		}

		// 3. Escrivimos los bytes recibidos en el archivo
		chunkSize, err := file.Write(req.GetData())
		if err != nil {
			return err
		}
		totalReceived += int64(chunkSize)

		//Publicar progreso en NATS
		s.Nats.Publish("file.progress", []byte(fmt.Sprintf("Subiendo %s: %d bytes recibidos", fileName, totalReceived)))
	}
}
