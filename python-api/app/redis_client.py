import redis.asyncio as redis
import os

REDIS_HOST = os.getenv(
    "REDIS_HOST",
    "redis"
)

r = redis.Redis(
    host=REDIS_HOST,
    port=6379,
    decode_responses=True
)


async def pause_file(file_id: str):

    await r.set(
        f"status:{file_id}",
        "PAUSED"
    )


async def resume_file(file_id: str):

    await r.set(
        f"status:{file_id}",
        "RUNNING"
    )