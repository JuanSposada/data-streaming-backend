from fastapi import APIRouter, File, UploadFile, Form,HTTPException
from fastapi.responses import StreamingResponse
import os

import asyncio

from app.grpc_client import GRPCClient
from app.redis_client import (
    pause_file,
    resume_file
)

router = APIRouter()

UPLOAD_DIR = "/uploads"

grpc_client = GRPCClient()

@router.get("/api/v1/files")
async def list_available_files():
    try:
        # Verificar si el directorio existe; si no, regresamos lista vacía
        if not os.path.exists(UPLOAD_DIR):
            return []
            
        # Listamos todo el contenido del directorio
        all_items = os.listdir(UPLOAD_DIR)
        
        # Filtrar para asegurarnos de listar SOLO archivos (ignorando carpetas temporales si las hay)
        files = [
            item for item in all_items 
            if os.path.isfile(os.path.join(UPLOAD_DIR, item))
        ]
        
        # Retornamos el array de strings directamente para que tu Vue lo mapee sin problemas
        return files
        
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Error al leer el almacenamiento local: {str(e)}"
        )

#  streaming real
async def stream_generator(file_id: str, start_chunk: int):
    stream = grpc_client.stream_file(file_id, start_chunk)
    
    try:
        while True:
            #Se delega la lecura del siguiente chun a un hilo para evitar el event loop
            chunk = await asyncio.to_thread(next, stream, None)

            if chunk is None:
                break
                
            yield chunk.data

        await asyncio.sleep(0.1) # Pequeña pausa para evitar bloqueos
    except Exception as e:
        print(f"Error en stream_generator: {e}")


#  endpoint download
@router.get("/api/v1/files/{file_id}")
async def stream_file(
    file_id: str,
    start_chunk: int = 0
):
    print(f"DEBUG API: Recibida petición para {file_id}. Empezando desde chunk: {start_chunk}")

    return StreamingResponse(
        stream_generator(
            file_id,
            start_chunk
        ),

        media_type="application/octet-stream",

        headers={
            "Content-Disposition":
            f"attachment; filename={file_id}",

            # IMPORTANTE
            "Access-Control-Expose-Headers":
            "Content-Length"
        }
    )

        


#  pause
@router.post("/api/v1/files/{file_id}/pause")
async def pause(file_id: str):

    await pause_file(file_id)

    return {
        "status": "paused"
    }


#  resume
@router.post("/api/v1/files/{file_id}/resume")
async def resume(file_id: str):
    print(f"DEBUG: Recibida petición de reanudar para {file_id}") # Mira los logs de api-1
    await resume_file(file_id)
    return {
        "status": "running"
    }

@router.get("/api/v1/files/{file_id}/info")
async def file_info(file_id: str):

    stream = grpc_client.stream_file(
        file_id,
        0
    )
    try:
        first_chunk = await asyncio.to_thread(next, stream, None)
        return {
            "total_chunks": first_chunk.total_chunks
        }
    except StopIteration:
        return {"error": "Archivo no encontrado"}

# Subida de archivos 
@router.post("/api/v1/files/upload")
async def upload_file(
    file: UploadFile = File(...),
    chunk_index: int = Form(...),
    total_chunks: int = Form(...)
):
    # Recibimos un chunk del front y lo envia a Go via gRPC.
    file_id = file.filename
    content = await file.read()

    # Enviamos el chunk al cliente gRPC que mantendra el stream
    # En tu endpoint de python-api/app/routes/files.py:
    success = await grpc_client.stream_chunk_to_go(file_id, content, chunk_index, total_chunks)

    if success: 
        return {"status": "chunk processed", "chunk": chunk_index}
    return {"status": "error processing chunk", "chunk": chunk_index}

    