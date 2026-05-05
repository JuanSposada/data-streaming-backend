# **🚀 Streaming Backend Core (Go \+ gRPC \+ NATS \+ Redis)**

Este es el núcleo del sistema de streaming diseñado para el manejo eficiente de archivos grandes mediante transferencia de datos binarios por trozos (chunks).

## **🛠 Arquitectura del Sistema**

El sistema utiliza un enfoque de microservicios coordinados:

* **gRPC:** Protocolo de transferencia de datos de alto rendimiento.  
* **Redis:** Orquestador de estado (Pausa/Reanudación).  
* **NATS:** Bus de eventos para observabilidad en tiempo real.  
* **Docker:** Entorno de ejecución contenedorizado.

---

## **📋 Manual de Integración para la API (Python/FastAPI)**

Para integrar tu API con este core, sigue estos lineamientos:

### **1\. Requisitos de Conexión**

* **Host gRPC:** `backend:50051` (dentro de Docker) o `localhost:50051` (local).  
* **Archivo Proto:** Utiliza `api/v1/transfer.proto` para generar tus stubs de Python con `grpcio-tools`.

### **2\. Flujo de Control (Redis)**

Para controlar el flujo de streaming desde la API, debes interactuar con Redis usando la siguiente convención de llaves:

* **Pausar:** `SET status:{file_id} "PAUSED"`  
* **Reanudar:** `SET status:{file_id} "RUNNING"`  
* **Consultar Progreso:** `GET last_chunk:{file_id}` (Devuelve el índice del último chunk procesado).

### **3\. Eventos del Sistema (NATS)**

La API o el Frontend pueden suscribirse a los siguientes *subjects* para monitoreo:

* `file.status`: Notifica cuando una descarga inicia, se pausa o termina.  
* `file.progress`: Emite el progreso actual (ID del archivo y número de chunk).  
* `file.errors`: Notifica fallos (archivo no encontrado o desconexión del cliente).

---

## **🚀 Guía de Inicio Rápido**

### **Levantar el entorno**

```Bash  
\# Construir e iniciar todos los servicios  
docker-compose up \--build
```
### **Probar el Bus de Eventos (NATS)**

Para espiar lo que sucede en el sistema sin afectar el flujo:

```Bash  
docker run --network streaming-project_default --rm -it natsio/nats-box nats sub --server nats:4222 "file.>"
```

### **Carga de Archivos**

Coloca los archivos que deseas streamear en la carpeta local `./uploads/`. El servidor los detectará automáticamente gracias al volumen montado en Docker.

---

## **🧪 Pruebas de Integridad (QA)**

Para asegurar que los archivos no se corrompen durante el streaming, compara los Hash SHA256:

1. **Original:** `sha256sum uploads/archivo.mp4`  
2. **Descargado:** `sha256sum descargado_archivo.mp4` *Ambos deben ser idénticos.*

---

### **Notas Finales para el Desarrollador de la API:**

* El servidor de Go maneja **Backpressure**: Si tu API deja de leer el stream, el servidor dejará de enviar datos automáticamente para proteger la memoria.  
* Los chunks tienen un tamaño fijo de **1MB**. Asegúrate de manejar los buffers adecuadamente en Python para no saturar el `StreamingResponse`.

