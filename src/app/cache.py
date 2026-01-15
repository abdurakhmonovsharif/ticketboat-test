# Use SSL in the AWS Production and Staging environments
from datetime import timedelta
import json
import logging
import os
from typing import Callable

import certifi
import redis

logger = logging.getLogger()
logger.setLevel("INFO")

# Configure Redis connection
if "amazonaws" in os.environ["REDIS_ADDRESS"]:
    # Production/Staging environment - use SSL
    redis_client = redis.Redis(
        host=os.environ["REDIS_ADDRESS"],
        port=int(os.environ["REDIS_PORT"], base=10),
        decode_responses=True,
        ssl=True,
        ssl_ca_certs=certifi.where(),
    )
else:
    # Local development - no SSL
    redis_client = redis.Redis(
        host=os.environ["REDIS_ADDRESS"],
        port=int(os.environ["REDIS_PORT"], base=10),
        decode_responses=True,
        ssl=False,
    )

shadows_redis_client = None


def get_shadows_redis_client():
    global shadows_redis_client
    if not shadows_redis_client:
        if "amazonaws.com" in os.environ["SHADOWS_REDIS_ADDRESS"]:
            # Production/Staging environment - use SSL
            shadows_redis_client = redis.StrictRedis(
                host=os.environ["SHADOWS_REDIS_ADDRESS"],
                port=int(os.environ["SHADOWS_REDIS_PORT"]),
                ssl=True,
                ssl_ca_certs=certifi.where(),
            )
        else:
            # Local development - no SSL
            shadows_redis_client = redis.StrictRedis(
                host=os.environ["SHADOWS_REDIS_ADDRESS"],
                port=int(os.environ["SHADOWS_REDIS_PORT"]),
                ssl=False,
            )
    return shadows_redis_client


async def handle_cache(key: str, timeout_secs: int, db_func: Callable, *args):
    results_json = redis_client.get(key)
    if results_json:
        logger.info(f"Hit Cache: {key}")
        results = json.loads(results_json)
    else:
        logger.info(f"Missed Cache: {key}")
        results = await db_func(*args)
        redis_client.setex(key, timedelta(seconds=timeout_secs), json.dumps(results, default=str))
    return results


def invalidate_cache(pattern: str):
    for key in redis_client.scan_iter(pattern):
        redis_client.delete(key)


def invalidate_shadows_cache(key: str):
    get_shadows_redis_client().delete(key)
