from fastapi import APIRouter
from fastapi.responses import StreamingResponse

import asyncio

from app.grpc_client import GRPCClient
from app.redis_client import (
    pause_file,
    resume_file
)

router = APIRouter()

grpc_client = GRPCClient()


#  streaming real
async def stream_generator(
    file_id: str,
    start_chunk: int
):

    stream = grpc_client.stream_file(
        file_id,
        start_chunk
    )

    for chunk in stream:

        #  bytes reales
        yield chunk.data

        await asyncio.sleep(0)


        #  endpoint download
        @router.get("/api/v1/files/{file_id}")
        async def stream_file(
            file_id: str,
            start_chunk: int = 0
        ):

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
        ),

        media_type="application/octet-stream",

            headers={
            "Content-Disposition":
            f"attachment; filename={file_id}"
        }
    )


#  pause
@router.post("/api/v1/files/{file_id}/pause")
def pause(file_id: str):

    pause_file(file_id)

    return {
        "status": "paused"
    }


#  resume
@router.post("/api/v1/files/{file_id}/resume")
def resume(file_id: str):

    resume_file(file_id)

    return {
        "status": "running"
    }

@router.get("/api/v1/files/{file_id}/info")
async def file_info(file_id: str):

    stream = grpc_client.stream_file(
        file_id,
        0
    )

    first_chunk = next(stream)

    return {
        "total_chunks": first_chunk.total_chunks
    }