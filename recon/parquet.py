"""Minimal zero-dependency Parquet reader/writer.

Scope (intentionally narrow but spec-compliant for the subset used here):
  * flat, required columns only (no definition/repetition levels)
  * physical types INT32 / INT64 / BYTE_ARRAY
  * PLAIN value encoding, one row group, one data page per column
  * UNCOMPRESSED or GZIP page compression
  * little-endian, Parquet v1 layout with a trailing Thrift Compact footer

The reader parses the footer generically (field-id keyed dicts) and also
tolerates files produced by mainstream engines as long as they stay inside
the subset above (e.g. GZIP/UNCOMPRESSED PLAIN pages, flat required schema).
"""
from __future__ import annotations

import gzip
import os
import struct

PARQUET_MAGIC = b"PAR1"

# Thrift Compact protocol type codes (per apache thrift compact_protocol)
_T_STOP = 0
_T_TRUE = 1
_T_FALSE = 2
_T_BYTE = 3
_T_I16 = 4
_T_I32 = 5
_T_I64 = 6
_T_DOUBLE = 7
_T_BINARY = 8
_T_LIST = 9
_T_MAP = 10
_T_STRUCT = 12
_T_SBYTE = 13

# Parquet enums
_PAGE_DATA_PAGE = 0
_ENC_PLAIN = 0
_REP_REQUIRED = 0
_CODEC_UNCOMPRESSED, _CODEC_GZIP = 0, 2
_TYPE_INT32, _TYPE_INT64, _TYPE_BYTE_ARRAY = 1, 2, 6


# --------------------------------------------------------------------------- #
# Thrift Compact encoding
# --------------------------------------------------------------------------- #
def _zz(n: int) -> int:
    """Zig-zag encoding for arbitrary-size python integers."""
    return (n << 1) ^ (-1 if n < 0 else 0)


def _write_uvarint(buf: bytearray, n: int) -> None:
    """Unsigned LEB128 varint (lengths, sizes, enum values)."""
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            buf.append(b | 0x80)
        else:
            buf.append(b)
            return


def _write_varint(buf: bytearray, n: int) -> None:
    """Signed zig-zag varint (compact-protocol integer fields)."""
    _write_uvarint(buf, _zz(n))


def _write_binary(buf: bytearray, b: bytes) -> None:
    _write_uvarint(buf, len(b))
    buf.extend(b)


class _CompactWriter:
    def __init__(self) -> None:
        self.buf = bytearray()

    # field ids tracked per nesting level
    def field(self, fid: int, last: int, ttype: int) -> int:
        delta = fid - last
        if 1 <= delta <= 15:
            self.buf.append((delta << 4) | ttype)
        else:
            self.buf.append(ttype)
            _write_varint(self.buf, fid)
        return fid

    def i32(self, fid: int, last: int, value: int) -> int:
        last = self.field(fid, last, _T_I32)
        _write_varint(self.buf, value)
        return last

    def i64(self, fid: int, last: int, value: int) -> int:
        last = self.field(fid, last, _T_I64)
        _write_varint(self.buf, value)
        return last

    def binary(self, fid: int, last: int, value: bytes) -> int:
        last = self.field(fid, last, _T_BINARY)
        _write_binary(self.buf, value)
        return last

    def boolean(self, fid: int, last: int, value: bool) -> int:
        return self.field(fid, last, _T_TRUE if value else _T_FALSE)

    def struct_begin(self) -> None:
        """Begin a struct-valued field or list element (no wrapper bytes)."""

    def struct_end(self) -> None:
        self.buf.append(_T_STOP)

    def list_begin(self, fid: int, last: int, elem_type: int, size: int) -> int:
        last = self.field(fid, last, _T_LIST)
        if size < 15:
            self.buf.append((size << 4) | elem_type)
        else:
            self.buf.append(0xF0 | elem_type)
            _write_uvarint(self.buf, size)
        return last


# --------------------------------------------------------------------------- #
# Thrift Compact decoding (generic: structs -> dict keyed by field id)
# --------------------------------------------------------------------------- #
class _CompactReader:
    def __init__(self, data: bytes, pos: int = 0, end: int | None = None) -> None:
        self.data = data
        self.pos = pos
        self.end = len(data) if end is None else end

    def read_varint(self) -> int:
        shift = 0
        result = 0
        while True:
            b = self.data[self.pos]
            self.pos += 1
            result |= (b & 0x7F) << shift
            if (b & 0x80) == 0:
                return result
            shift += 7

    @staticmethod
    def _unzig(n: int) -> int:
        return (n >> 1) ^ -(n & 1)

    def read_zzint(self) -> int:
        return self._unzig(self.read_varint())

    def read_binary(self) -> bytes:
        n = self.read_varint()
        s = self.data[self.pos:self.pos + n]
        self.pos += n
        return s

    def read_struct(self) -> dict:
        out: dict = {}
        last_id = 0
        while True:
            header = self.data[self.pos]
            self.pos += 1
            if header == _T_STOP:
                return out
            ttype = header & 0x0F
            delta = header >> 4
            if delta:
                fid = last_id + delta
            else:
                fid = self.read_zzint()
            last_id = fid
            out[fid] = self.read_value(ttype)

    def read_value(self, ttype: int):
        if ttype == _T_TRUE:
            return True
        if ttype == _T_FALSE:
            return False
        if ttype in (_T_BYTE, _T_SBYTE):
            v = self.data[self.pos]
            self.pos += 1
            return v - 256 if ttype == _T_SBYTE and v > 127 else v
        if ttype in (_T_I16, _T_I32, _T_I64):
            return self.read_zzint()
        if ttype == _T_DOUBLE:
            v = struct.unpack("<d", self.data[self.pos:self.pos + 8])[0]
            self.pos += 8
            return v
        if ttype == _T_BINARY:
            return self.read_binary()
        if ttype == _T_STRUCT:
            return self.read_struct()
        if ttype == _T_LIST:
            # On entry self.pos points at the (not yet consumed) list header.
            header = self.data[self.pos]
            self.pos += 1
            size = header >> 4
            elem_type = header & 0x0F
            if size == 15:
                size = self.read_varint()
            return [self.read_value(elem_type) for _ in range(size)]
        if ttype == _T_MAP:
            size = self.read_varint()
            if size == 0:
                return {}
            kv = self.data[self.pos]
            self.pos += 1
            kt, vt = kv >> 4, kv & 0x0F
            return {self.read_value(kt): self.read_value(vt) for _ in range(size)}
        raise ValueError(f"unsupported thrift compact type {ttype}")


# --------------------------------------------------------------------------- #
# Parquet writer
# --------------------------------------------------------------------------- #
def _encode_plain(ptype: int, values: list) -> bytes:
    if ptype == _TYPE_INT64:
        return b"".join(struct.pack("<q", int(v)) for v in values)
    if ptype == _TYPE_INT32:
        return b"".join(struct.pack("<i", int(v)) for v in values)
    if ptype == _TYPE_BYTE_ARRAY:
        return b"".join(struct.pack("<I", len(v.encode("utf-8"))) + v.encode("utf-8")
                        for v in values)
    raise ValueError(f"unsupported physical type {ptype}")


def _build_page_header(num_values: int, raw_size: int, comp_size: int) -> bytes:
    w = _CompactWriter()
    last = 0
    last = w.i32(1, last, _PAGE_DATA_PAGE)
    last = w.i32(2, last, raw_size)
    last = w.i32(3, last, comp_size)
    last = w.list_begin(5, last, _T_STRUCT, 1)
    inner = _CompactWriter()
    ilast = 0
    ilast = inner.i32(1, ilast, num_values)
    ilast = inner.i32(2, ilast, _ENC_PLAIN)
    ilast = inner.i32(3, ilast, 3)
    ilast = inner.i32(4, ilast, 0)
    inner.struct_end()
    w.buf.extend(bytes(inner.buf))          # DataPageHeader already has its STOP
    w.struct_end()                          # PageHeader's own STOP
    return bytes(w.buf)


def _schema_element(name: str, ptype: int | None, num_children: int | None,
                    root: bool = False) -> bytes:
    w = _CompactWriter()
    last = 0
    if ptype is not None:
        last = w.i32(1, last, ptype)          # Type
    last = w.i32(3, last, _REP_REQUIRED)      # repetition type
    last = w.binary(4, last, name.encode("utf-8"))
    if num_children is not None:
        last = w.i32(5, last, num_children)
    w.struct_end()
    return bytes(w.buf)


def write_table(path: str, columns: dict[str, list],
                compression: str = "gzip") -> None:
    """Write a Parquet file. Column values must be int or str; equal lengths."""
    names = list(columns.keys())
    nrows = len(columns[names[0]]) if names else 0
    for name in names:
        if len(columns[name]) != nrows:
            raise ValueError(f"column {name!r} length mismatch")
    ptypes = []
    for name in names:
        vals = columns[name]
        if all(isinstance(v, int) and not isinstance(v, bool) for v in vals) or not vals:
            ptypes.append(_TYPE_INT64)
        elif all(isinstance(v, str) for v in vals):
            ptypes.append(_TYPE_BYTE_ARRAY)
        else:
            raise ValueError(f"column {name!r}: only int/str values supported")

    codec = _CODEC_GZIP if compression == "gzip" else _CODEC_UNCOMPRESSED
    out = bytearray(PARQUET_MAGIC)
    row_group_columns: list[bytes] = []
    total_byte_size = 0

    for name, ptype in zip(names, ptypes):
        raw = _encode_plain(ptype, columns[name])
        if codec == _CODEC_GZIP:
            compressed = gzip.compress(raw)
        else:
            compressed = raw
        offset = len(out)
        header = _build_page_header(nrows, len(raw), len(compressed))
        out.extend(header)
        out.extend(compressed)
        total_uncomp = len(header) + len(raw)
        total_comp = len(header) + len(compressed)
        total_byte_size += total_uncomp

        # ColumnMetaData struct
        cm = _CompactWriter()
        last = 0
        last = cm.i32(1, last, ptype)
        last = cm.list_begin(2, last, _T_I32, 1)          # encodings
        _write_varint(cm.buf, _ENC_PLAIN)
        last = cm.list_begin(3, last, _T_BINARY, 1)       # path_in_schema
        _write_binary(cm.buf, name.encode("utf-8"))
        last = cm.i32(4, last, codec)
        last = cm.i64(5, last, nrows)
        last = cm.i64(6, last, total_uncomp)
        last = cm.i64(7, last, total_comp)
        last = cm.i64(10, last, offset)                    # data_page_offset
        cm.struct_end()

        cc = _CompactWriter()
        last = 0
        last = cc.i64(2, last, offset)                     # file_offset
        last = cc.list_begin(3, last, _T_STRUCT, 1)       # meta_data
        cc.buf.extend(bytes(cm.buf))                     # cm already has its STOP
        cc.struct_end()                                  # ColumnChunk's own STOP
        row_group_columns.append(bytes(cc.buf))

    # RowGroup
    rg = _CompactWriter()
    last = 0
    last = rg.list_begin(1, last, _T_STRUCT, len(row_group_columns))
    for chunk in row_group_columns:
        rg.buf.extend(chunk)               # each chunk already ends with its STOP
    last = rg.i64(2, last, total_byte_size)
    last = rg.i64(3, last, nrows)
    rg.struct_end()

    # FileMetaData
    fm = _CompactWriter()
    last = 0
    last = fm.i32(1, last, 1)                                        # version
    last = fm.list_begin(2, last, _T_STRUCT, len(names) + 1)         # schema
    fm.buf.extend(_schema_element("schema", None, len(names), root=True))
    for name, ptype in zip(names, ptypes):
        fm.buf.extend(_schema_element(name, ptype, None))
    last = fm.i64(3, last, nrows)
    last = fm.list_begin(4, last, _T_STRUCT, 1)                      # row groups
    fm.buf.extend(bytes(rg.buf))                     # rg already has its STOP
    fm.struct_end()                                  # FileMetaData's own STOP
    footer = bytes(fm.buf)

    out.extend(footer)
    out.extend(struct.pack("<I", len(footer)))
    out.extend(PARQUET_MAGIC)
    parent = os.path.dirname(os.path.abspath(path))
    os.makedirs(parent, exist_ok=True)
    with open(path, "wb") as f:
        f.write(bytes(out))


# --------------------------------------------------------------------------- #
# Parquet reader
# --------------------------------------------------------------------------- #
def read_table(path: str) -> dict[str, list]:
    """Read a flat required-schema Parquet file into {column: [values]}."""
    with open(path, "rb") as f:
        data = f.read()
    if data[:4] != PARQUET_MAGIC or data[-4:] != PARQUET_MAGIC:
        raise ValueError(f"{path}: not a parquet file (bad magic)")
    footer_len = struct.unpack("<I", data[-8:-4])[0]
    footer = _CompactReader(data, len(data) - 8 - footer_len,
                            len(data) - 8).read_struct()

    # FileMetaData: 2 schema, 4 row groups
    schema = footer[2]
    col_names = [se[4].decode("utf-8") for se in schema if 1 in se]
    result: dict[str, list] = {name: [] for name in col_names}

    for rg in footer.get(4, []):
        chunks = rg[1]
        for name, chunk in zip(col_names, chunks):
            meta = chunk[3][0]  # meta_data is a list of one ColumnMetaData struct
            ptype = meta[1]
            codec = meta[4]
            num_values = meta[5]
            offset = meta[10]
            r = _CompactReader(data, offset)
            page_header = r.read_struct()
            comp_size = page_header[3]
            payload = data[r.pos:r.pos + comp_size]
            if codec == _CODEC_GZIP:
                raw = gzip.decompress(payload)
            elif codec == _CODEC_UNCOMPRESSED:
                raw = payload
            else:
                raise ValueError(f"unsupported compression codec {codec}")
            vals = _decode_plain(ptype, raw, num_values)
            if len(vals) != num_values:
                raise ValueError(f"{path}: page value count mismatch")
            result[name].extend(vals)
    return result


def _decode_plain(ptype: int, raw: bytes, num_values: int) -> list:
    if ptype == _TYPE_INT64:
        return [struct.unpack("<q", raw[i * 8:(i + 1) * 8])[0]
                for i in range(num_values)]
    if ptype == _TYPE_INT32:
        return [struct.unpack("<i", raw[i * 4:(i + 1) * 4])[0]
                for i in range(num_values)]
    if ptype == _TYPE_BYTE_ARRAY:
        out = []
        pos = 0
        for _ in range(num_values):
            (n,) = struct.unpack("<I", raw[pos:pos + 4])
            pos += 4
            out.append(raw[pos:pos + n].decode("utf-8"))
            pos += n
        return out
    raise ValueError(f"unsupported physical type {ptype}")
