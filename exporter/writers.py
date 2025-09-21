import json
from typing import Any, Dict, List, Optional

from .utils import to_string

_PARQUET_AVAILABLE = False
_AVRO_AVAILABLE = False
_ORC_AVAILABLE = False

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    _PARQUET_AVAILABLE = True
except Exception:
    pass

try:
    from fastavro import writer as avro_writer
    from fastavro import parse_schema as avro_parse_schema
    _AVRO_AVAILABLE = True
except Exception:
    pass

try:
    import pyorc
    _ORC_AVAILABLE = True
except Exception:
    pass


class BaseBatchWriter:
    def write_batch(self, rows: List[Dict[str, Any]]) -> None:
        raise NotImplementedError

    def close(self) -> None:
        pass


class JSONLWriter(BaseBatchWriter):
    def __init__(self, file_path: str):
        self.file = open(file_path, "w", encoding="utf-8")

    def write_batch(self, rows: List[Dict[str, Any]]) -> None:
        for row in rows:
            self.file.write(json.dumps(row, ensure_ascii=False) + "\n")

    def close(self) -> None:
        self.file.close()


class CSVWriter(BaseBatchWriter):
    def __init__(self, file_path: str, header_fields: Optional[List[str]] = None):
        import csv
        self.csv = csv
        self.file = open(file_path, "w", encoding="utf-8", newline="")
        self.header_fields = header_fields
        self.writer = None

    def write_batch(self, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return
        if self.writer is None:
            if self.header_fields is None:
                header_set = set()
                for r in rows:
                    header_set.update(r.keys())
                self.header_fields = sorted(header_set)
            self.writer = self.csv.DictWriter(
                self.file, fieldnames=self.header_fields)
            self.writer.writeheader()
        for r in rows:
            safe_row = {k: to_string(r.get(k)) for k in self.header_fields}
            self.writer.writerow(safe_row)

    def close(self) -> None:
        self.file.close()


class ParquetWriter(BaseBatchWriter):
    def __init__(self, file_path: str):
        if not _PARQUET_AVAILABLE:
            raise RuntimeError("pyarrow is required for parquet output")
        self.file_path = file_path
        self._writer = None

    def write_batch(self, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return
        converted = [{k: to_string(v) for k, v in r.items()} for r in rows]
        table = pa.Table.from_pylist(converted)
        if self._writer is None:
            self._writer = pq.ParquetWriter(self.file_path, table.schema)
        self._writer.write_table(table)

    def close(self) -> None:
        if self._writer is not None:
            self._writer.close()


class AvroWriter(BaseBatchWriter):
    def __init__(self, file_path: str):
        if not _AVRO_AVAILABLE:
            raise RuntimeError("fastavro is required for avro output")
        self.file_path = file_path
        self._schema = None
        self._fh = open(file_path, "wb")

    def _ensure_schema(self, rows: List[Dict[str, Any]]):
        if self._schema is not None:
            return
        fields = []
        keys: set = set()
        for r in rows:
            keys.update(r.keys())
        for k in sorted(keys):
            fields.append(
                {"name": k, "type": ["null", "string"], "default": None})
        self._schema = avro_parse_schema({
            "type": "record",
            "name": "MongoExport",
            "fields": fields,
        })

    def write_batch(self, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return
        self._ensure_schema(rows)
        field_names = list(self._schema["fields_by_name"].keys())
        to_write = []
        for r in rows:
            to_write.append(
                {k: None if r.get(k) is None else to_string(r.get(k)) for k in field_names})
        avro_writer(self._fh, self._schema, to_write)

    def close(self) -> None:
        try:
            self._fh.close()
        except Exception:
            pass


class ORCWriter(BaseBatchWriter):
    def __init__(self, file_path: str):
        if not _ORC_AVAILABLE:
            raise RuntimeError("pyorc is required for orc output")
        self.file_path = file_path
        self._schema = None
        self._fh = open(file_path, "wb")
        self._writer = None

    def _ensure_schema(self, rows: List[Dict[str, Any]]):
        if self._writer is not None:
            return
        keys: set = set()
        for r in rows:
            keys.update(r.keys())
        schema_str = "struct<" + \
            ",".join([f"{k}:string" for k in sorted(keys)]) + ">"
        self._schema = pyorc.TypeDescription(schema_str)
        self._writer = pyorc.Writer(self._fh, self._schema)

    def write_batch(self, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return
        self._ensure_schema(rows)
        field_names = [f.split(":")[0]
                       for f in str(self._schema)[7:-1].split(",")]
        for r in rows:
            self._writer.write(tuple(to_string(r.get(k)) for k in field_names))

    def close(self) -> None:
        try:
            if self._writer is not None:
                self._writer.close()
            self._fh.close()
        except Exception:
            pass


def make_writer(fmt: str, output_path: str, header_fields: Optional[List[str]] = None) -> BaseBatchWriter:
    fmt_lower = fmt.lower()
    if fmt_lower == "jsonl":
        return JSONLWriter(output_path)
    if fmt_lower == "csv":
        return CSVWriter(output_path, header_fields=header_fields)
    if fmt_lower == "parquet":
        return ParquetWriter(output_path)
    if fmt_lower == "avro":
        return AvroWriter(output_path)
    if fmt_lower == "orc":
        return ORCWriter(output_path)
    raise ValueError(f"Unsupported format: {fmt}")
