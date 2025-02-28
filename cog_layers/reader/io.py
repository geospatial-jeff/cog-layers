import os
from dataclasses import dataclass
import typing
import functools

import aiohttp
import obstore as obs


RangeRequestFuncType = typing.Callable[[str, str, int, int, typing.Any], typing.Awaitable[bytes]]


@functools.lru_cache(maxsize=1)
def _get_default_aiohttp_client(**kwargs) -> aiohttp.ClientSession:
    """Default aiohttp client with a singleton cache."""
    return aiohttp.ClientSession(**kwargs)


async def send_range_aiohttp(bucket: str, key: str, start: int, end: int, client: typing.Any | None = None) -> bytes:
    raise NotImplementedError()


@functools.lru_cache(maxsize=1)
def _get_default_obstore_client(bucket_name: str, **kwargs) -> obs.store.S3Store:
    """Default obstore client with a singleton cache."""
    return obs.store.S3Store(bucket_name, **kwargs)


async def send_range_obstore(bucket:str, key:str, start: int, end: int, client: typing.Any | None = None) -> bytes:
    """Send a range request with obstore.
    
    Creates a single client and reuses unless overridden by caller.
    """
    client = client or _get_default_obstore_client(bucket)
    r = await obs.get_range_async(client, key, start=start, end=end)
    return r.to_bytes()
