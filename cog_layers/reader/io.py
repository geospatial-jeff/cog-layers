import os
from dataclasses import dataclass
import typing

import aiofiles
import obstore as obs


RangeRequestFuncType = typing.Callable[[str, str, int, int], typing.Awaitable[bytes]]


# TODO: this should be bucket,key
# function responsible for translating to https qualified url if needed.
async def send_range_local(bucket, key, start, end) -> bytes:
    filepath = os.path.join(bucket, key)
    async with aiofiles.open(filepath, 'rb') as f:
        await f.seek(start)
        b = await f.read(end - start)
        return b


async def send_range_aiohttp(bucket, key, start, end) -> bytes:
    raise NotImplementedError()


async def send_range_obstore(bucket, key, start, end) -> bytes:
    store = obs.store.S3Store(
        bucket,
        config={"aws_default_region": "us-west-2", "aws_skip_signature": True}
    )
    r = await obs.get_range_async(store, key, start=start, end=end)
    return r.to_bytes()

