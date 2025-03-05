import math
import typing
import struct
import functools

from cog_layers.reader.types import (
    Endian,
    Tag,
    Header,
    TiffVersion,
    _ENDIAN_BYTES,
    _TAG_TYPES,
    TagType,
    IFD,
    Cog,
)
from cog_layers.reader.io import RangeRequestFuncType


@functools.lru_cache
def _get_tag_cls(code: int) -> typing.Type[Tag] | None:
    """Fetch the appropriate tag class by code."""
    available_tags = {t.id: t for t in Tag.__subclasses__()}
    return available_tags.get(code, None)


def _get_endian(b: bytes) -> Endian:
    """Lookup endian."""
    return _ENDIAN_BYTES[b]


def _get_tag_type(code: int) -> TagType:
    """Lookup tag type."""
    return _TAG_TYPES[code]


def _add_jpeg_tables(tile: bytes, ifd: IFD, endian: Endian):
    """Append JPEG tables to the tile, making it a valid JPEG image"""
    jpeg_tables = ifd.tags["JPEGTables"]
    encoded_jpeg_tables = struct.pack(
        f"{endian.value}{jpeg_tables.count}{jpeg_tables.type.format}",
        *jpeg_tables.value,
    )
    if tile[0] == 0xFF and tile[1] == 0xD8:
        # insert tables, first removing the SOI and EOI
        return tile[0:2] + encoded_jpeg_tables[2:-2] + tile[2:]
    else:
        raise Exception("Missing SOI marker for JPEG tile")


async def open_cog(
    callable: RangeRequestFuncType,
    bucket: str,
    key: str,
    header_size_bytes: int = 32768,
) -> Cog:
    """Open a Cloud Optimized GeoTiff using the supplied callable.  `header_size_bytes` must be set large
    enough to read the entire header or this code will probably break.
    """
    b = await callable(bucket, key, start=0, end=header_size_bytes)

    # Read the header
    endian = _get_endian(b[:2])
    header = Header(
        endian=endian,
        version=TiffVersion.from_bytes(b[2:4], endian.name),
        first_ifd_offset=int.from_bytes(b[4:8], endian.name),
    )

    # Find the offset of the first IFD.
    next_ifd_offset = header.first_ifd_offset

    # Read each IFD, the last IFD in the image will have a next IFD offset of 0.
    ifds = []
    while next_ifd_offset != 0:
        # First 2 bytes contain number of tags in the IFD.
        tag_count = int.from_bytes(
            b[next_ifd_offset : next_ifd_offset + 2], header.endian.name
        )

        # Read each tag.
        tags = {}
        for idx in range(tag_count):
            # Tags are always 12 bytes each.
            tag_start = next_ifd_offset + 2 + (12 * idx)

            # First 2 bytes contain tag code.
            tag_code = int.from_bytes(b[tag_start : tag_start + 2], header.endian.name)
            tag_cls = _get_tag_cls(tag_code)

            if not tag_cls:
                continue

            # Bytes 2-4 contain the tag's field type.
            data_type = int.from_bytes(
                b[tag_start + 2 : tag_start + 4], header.endian.name
            )
            tag_type = _get_tag_type(data_type)

            # Bytes 4-8 contain the number of values in the tag.
            # We use this to determine the overall size of the tag's value.
            count = int.from_bytes(b[tag_start + 4 : tag_start + 8], header.endian.name)
            size = count * tag_type.length

            # Bytes 8-12 contain the tag value if it fits, otherwise it contains
            # an offset to where the tag value is stored.
            if size <= 4:
                tag_value = b[tag_start + 8 : tag_start + 8 + size]
            else:
                value_offset = int.from_bytes(
                    b[tag_start + 8 : tag_start + 12], header.endian.name
                )
                tag_value = b[value_offset : value_offset + size]

            # Decode the tag's value based on field type.
            decoded_tag_value = struct.unpack(
                f"{header.endian.value}{count}{tag_type.format}", tag_value
            )
            tag = tag_cls(
                count=count, type=tag_type, size=size, value=decoded_tag_value
            )
            tags[tag.name] = tag

        # Last 4 bytes of IFD contains offset to the next IFD.
        next_ifd_offset = int.from_bytes(
            b[tag_start + 12 : tag_start + 12 + 4], header.endian.name
        )
        ifds.append(
            IFD(tag_count=tag_count, next_ifd_offset=next_ifd_offset, tags=tags)
        )

    return Cog(bucket, key, header, ifds, _send_range_request=callable)


async def read_tile(x: int, y: int, z: int, cog: Cog) -> bytes:
    """Read a single tile from an IFD.

    Inputs are expressed in tile coordinates relative to the top-left corner
    of the image, while `z` is used to select the IFD.
    """
    # Calculate number of columns in the IFD.
    ifd = cog.ifds[z]
    image_width = ifd.tags["ImageWidth"].value[0]
    tile_width = ifd.tags["TileWidth"].value[0]
    columns = math.ceil(image_width / tile_width)

    # Tiles are stored row-major.
    idx = (y * columns) + x
    tile_offset = ifd.tags["TileOffsets"].value[idx]
    tile_byte_count = ifd.tags["TileByteCounts"].value[idx]

    # Read the tile.
    b = await cog._send_range_request(
        cog.bucket, cog.key, tile_offset, tile_offset + tile_byte_count
    )
    b = _add_jpeg_tables(b, ifd, cog.header.endian)
    return b


async def read_row(
    y: int, z: int, cog: Cog, x_start: int | None = None, x_end: int | None = None
) -> list[bytes]:
    """Read a row of tiles, merging all ranges.

    Inputs are expressed in tile coordinates relative to the top-left corner
    of the image.  Optionally return only tiles between `x_start` and
    `x_end` (inclusive).
    """
    ifd = cog.ifds[z]
    image_width = ifd.tags["ImageWidth"].value[0]
    tile_width = ifd.tags["TileWidth"].value[0]
    columns = math.ceil(image_width / tile_width)

    if x_start is not None:
        idx_start = (y * columns) + x_start
    else:
        idx_start = y * columns

    if x_end is not None:
        idx_end = (y * columns) + x_end
    else:
        idx_end = (y * columns) + int((image_width / tile_width))

    # Read from the start of the first tile to the end of the last.
    tile_offsets = ifd.tags["TileOffsets"].value
    tile_byte_counts = ifd.tags["TileByteCounts"].value
    range_start = tile_offsets[idx_start]
    range_end = tile_offsets[idx_end] + tile_byte_counts[idx_end]
    b = await cog._send_range_request(cog.bucket, cog.key, range_start, range_end)

    # Break the row into indiviual tiles.  Each tile is compressed individually,
    # and the caller doesn't (easily) have the information to do this themselves.
    tiles = []
    for i in range(idx_start, idx_end + 1):
        relative_offset = tile_offsets[i] - range_start
        tile_content = b[relative_offset : relative_offset + tile_byte_counts[i]]
        tile_content = _add_jpeg_tables(tile_content, ifd, cog.header.endian)
        tiles.append(tile_content)
    return tiles
