import redis
from typing import Any, Dict, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

    
class RedisClientSingleton:
    """Singleton Redis client wrapper."""

    _instance: Optional[redis.Redis] = None

    @classmethod
    def get_client(cls) -> redis.Redis:
        if cls._instance is None:
            try:
                # connecting to upstash
                # cls._instance = redis.Redis.from_url(REDIS_URL,
                #     decode_responses=True,  # return str instead of bytes
                #     socket_timeout=2,
                #     health_check_interval=30,
                # )

                # connecting locally
                cls._instance = redis.Redis(
                    host="127.0.0.1",
                    port=6379,
                    decode_responses=True,  # return str instead of bytes
                    socket_timeout=2,
                    health_check_interval=30,
                )
                # Test the connection
                cls._instance.ping()
                logger.info("Connected to Redis successfully.")
            except ConnectionError as e:
                logger.error("Failed to connect to Redis: %s", e)
                raise
        return cls._instance
