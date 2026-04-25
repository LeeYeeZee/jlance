"""Milestone 61: Mixed-layout (MiniBlock + ConstantLayout) multi-page list<int>."""

import os
import pyarrow as pa
import lance.dataset
import lance.file

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "milestone_61")
os.makedirs(DATA_DIR, exist_ok=True)

schema = pa.schema([pa.field("items", pa.list_(pa.int32()), nullable=True)])

# Data with a mix of non-empty lists, empty lists, and nulls to trigger
# both MiniBlock and ConstantLayout pages across the column.
# Small max_page_bytes forces multi-page output.
data = [
    [1, 2, 3],
    [],
    None,
    [4, 5],
    [],
    [6],
    None,
    [7, 8, 9, 10],
    [],
    [11],
]

table = pa.table({"items": data}, schema=schema)

path = os.path.join(DATA_DIR, "test_mixed_layout_list.lance")
if os.path.exists(path):
    os.remove(path)
with lance.file.LanceFileWriter(path, table.schema, version="2.1", max_page_bytes=64) as writer:
    writer.write_batch(table)
print(f"Created {path} with {table.num_rows} rows")
print("Schema:", table.schema)
