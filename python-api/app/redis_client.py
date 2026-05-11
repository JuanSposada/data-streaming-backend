import redis
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


def pause_file(file_id: str):

    r.set(
        f"status:{file_id}",
        "PAUSED"
    )


def resume_file(file_id: str):

    r.set(
        f"status:{file_id}",
        "RUNNING"
    )