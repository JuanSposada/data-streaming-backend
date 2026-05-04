# etapa de compilacion
FROM golang:1.26-alpine AS builder
WORKDIR /app
# Copiar archivos y dependencias
COPY go.mod go.sum ./
RUN go mod download
# Copiar el codigo fuente
COPY . .
# Compilar el binario para el servidor
RUN go build -o main ./cmd/server/main.go

# Etapa de ejecucion
FROM alpine:latest
WORKDIR /root/
# copiar el binario desde la etapa de complilacion
COPY --from=builder /app/main .
# crear carpeta de uploads por si no existe
RUN mkdir ./uploads
# Exponer el puerto gRPC
EXPOSE 50051
CMD ["./main"]