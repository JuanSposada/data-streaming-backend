import grpc
import os
import asyncio

from generated import transfer_pb2
from generated import transfer_pb2_grpc


class GRPCClient:

    def __init__(self):

        #  host Docker
        grpc_host = os.getenv(
            "GRPC_HOST",
            "backend:50051"
        )

        #  canal gRPC
        self.channel = grpc.insecure_channel(grpc_host)

        #  stub
        self.stub = (
            transfer_pb2_grpc.FileServiceStub(
                self.channel
            )
        )

    #  stream file
    def stream_file(
        self,
        file_id,
        start_chunk=0
    ):

        request = transfer_pb2.FileRequest(
            file_id=file_id,
            start_chunk=start_chunk
        )

        return self.stub.StreamFile(request)
    

    async def stream_chunk_to_go(
        self,
        file_id: str,
        content: bytes,
        chunk_index: int,
        total_chunks: int
    ) -> bool:

        try:
            def request_generator():
                yield transfer_pb2.UploadRequest(
                    file_name=file_id,
                    data=content,
                    chunk_index=chunk_index # 🔥 ¡AQUÍ LO METES!
                )
            # 2. Como tu canal gRPC actual es sincrónico, envolvemos la llamada 
            # en un hilo para mantener la API asíncrona y rápida
            response = await asyncio.to_thread(
                self.stub.UploadFile, 
                request_generator()
            )
            if chunk_index + 1 == total_chunks:
                print(f"DEBUG gRPC CLIENT: Último chunk ({chunk_index + 1}/{total_chunks}) enviado a Go.")
            
            return response.success

        except Exception as e:
            print(f"ERROR en GRPCClient (upload): {e}")
            return False