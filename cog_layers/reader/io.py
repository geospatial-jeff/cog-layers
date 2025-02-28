import os
from dataclasses import dataclass
import typing

import aiofiles
import obstore as obs


RangeRequestFuncType = typing.Callable[[str, str, int, int], typing.Awaitable[bytes]]


async def send_range_aiohttp(bucket, key, start, end) -> bytes:
    raise NotImplementedError()


async def send_range_obstore(bucket, key, start, end) -> bytes:
    store = obs.store.S3Store(
        bucket,
        config={"aws_default_region": "us-west-2", "aws_skip_signature": True}
    )
    r = await obs.get_range_async(store, key, start=start, end=end)
    return r.to_bytes()

