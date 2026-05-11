from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routes import files

#  Crear app
app = FastAPI(
    title="Streaming API Gateway",
    version="1.0.0"
)

#  CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

#  Registrar rutas
app.include_router(files.router)

#  Endpoint healthcheck
@app.get("/")
async def root():
    return {
        "status": "running",
        "service": "python-api"
    }