from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routes import files

#  Crear app
app = FastAPI(
    title="Streaming API Gateway",
    version="1.0.0"
)

origins = [
    "http://localhost:5173",  # Tu puerto de Vue
    "http://127.0.0.1:5173",
    "*",                      # Permitir todos temporalmente para pruebas de tesis
]

#  CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition", "Content-Length"],
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