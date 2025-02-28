import asyncio
import math

import mercantile
from cog_layers.reader.cog import read_tile, read_row
from cog_layers.reader.types import Cog


def get_seed_tile(cog: Cog):
    quadkey = cog.key.split("/")[-2]
    return mercantile.quadkey_to_tile(quadkey)


async def request_xyz_tile(cog: Cog, tile: mercantile.Tile) -> bytes:
    """Request a single XYZ tile from the COG."""
    seed_tile = get_seed_tile(cog)
    overview_level = seed_tile.z + len(cog.ifds) - tile.z - 1
    origin = mercantile.tile(*mercantile.ul(seed_tile), tile.z)
    xoff = tile.x - origin.x
    yoff = tile.y - origin.y
    return await read_tile(xoff, yoff, overview_level, cog)


async def request_metatile(cog: Cog, tile: mercantile.Tile, size: int) -> list[list[bytes]]:
    """Request a single metatile from the COG.
    
    A metatile is a "tile of tiles".  For example a Z10 tile with a size of 4 contains
    all Z8 children of the parent Z10 tile.  The metatile always has number of tiles
    equal to `size ** 2`, with a zoom of log(size, 2).  Assuming a decimation of 2.

    A larger metatile size will result in larger (and fewer) range requests.  They provide an
    O(n**2) reduction in range requests compared to requesting individual tiles.  For example,
    a metatile of size 2 requires 2 range requests to read 4 tiles while a metatile size of 4
    requires 4 range requests to read 16 tiles etc.

    Note that a numpy array is a better return type for this function but I haven't implemented
    decompression yet so we are currently returning a list[list[bytes]] where the top level list
    contains each row of tiles, and the sublists contain each tile within each row.
    """
    seed_tile = get_seed_tile(cog)
    metatile_zoom = int(tile.z + math.log(size, 2))
    origin = mercantile.tile(*mercantile.ul(seed_tile), metatile_zoom)
    children = mercantile.children(tile, zoom=metatile_zoom)
    xs, ys = zip(*[(child.x - origin.x, child.y - origin.y) for child in children])
    unique_ys = list(set(ys))
    overview_level = metatile_zoom - seed_tile.z - 1
    return await asyncio.gather(*[
        read_row(y, overview_level, cog, min(xs), max(xs)) for y in unique_ys
    ])

