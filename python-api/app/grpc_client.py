import grpc
import os

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