"""Milestone 60: nullable struct<nullable int> — stacked NullableItem."""

import os
import pyarrow as pa
import lance

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "milestone_60")
os.makedirs(DATA_DIR, exist_ok=True)

struct_type = pa.struct([pa.field("x", pa.int32(), nullable=True)])
schema = pa.schema([pa.field("s", struct_type, nullable=True)])

data = [
    {"x": 1},
    {"x": None},
    None,
    {"x": 10},
    None,
]

table = pa.table({"s": data}, schema=schema)

path = os.path.join(DATA_DIR, "test_nullable_struct_nullable_int.lance")
if os.path.exists(path):
    os.remove(path)
with lance.file.LanceFileWriter(path, table.schema, version="2.1") as writer:
    writer.write_batch(table)
print(f"Created {path} with {table.num_rows} rows")
print("Schema:", table.schema)
