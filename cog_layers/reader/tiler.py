import asyncio
import math

import mercantile
from cog_layers.reader.cog import read_tile, read_row
from cog_layers.reader.types import Cog


async def request_xyz_tile(cog: Cog, tile: mercantile.Tile) -> bytes:
    """Request a single XYZ tile from the COG."""
    quadkey = cog.key.split("/")[-2]
    parent_tile = mercantile.quadkey_to_tile(quadkey)
    overview_level = parent_tile.z + len(cog.ifds) - tile.z - 1
    origin = mercantile.tile(*mercantile.ul(parent_tile), tile.z)
    xoff = tile.x - origin.x
    yoff = tile.y - origin.y
    return await read_tile(xoff, yoff, overview_level, cog)


async def request_metatile(cog: Cog, tile: mercantile.Tile, size: int) -> list[list[bytes]]:
    """Request a single metatile from the COG.
    
    A metatile is a "tile of tiles".  For example a Z10 tile with a size of 4 contains
    all Z8 children of the parent Z10 tile.  The metatile always has number of tiles
    equal to `size ** 2`, assuming a decimation of 2.

    A larger metatile size will result in larger (and fewer) range requests
    """
    quadkey = cog.key.split("/")[-2]
    parent_tile = mercantile.quadkey_to_tile(quadkey)
    metatile_zoom = int(tile.z + math.log(size, 2))
    origin = mercantile.tile(*mercantile.ul(parent_tile), metatile_zoom)
    children = mercantile.children(tile, zoom=metatile_zoom)
    xs, ys = zip(*[(child.x - origin.x, child.y - origin.y) for child in children])
    xmin = min(xs)
    xmax = max(xs)
    unique_ys = list(set(ys))
    overview_level = metatile_zoom - parent_tile.z - 1
    return await asyncio.gather(*[
        read_row(y, overview_level, cog, xmin, xmax) for y in unique_ys
    ])

