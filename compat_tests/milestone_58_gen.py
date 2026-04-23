"""Milestone 58: 5-level mixed list/struct nesting."""

import os
import pyarrow as pa
import lance

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "milestone_58")
os.makedirs(DATA_DIR, exist_ok=True)

# Depth: list -> struct -> list -> struct -> list -> int  (5 layers)
inner_struct = pa.struct([
    pa.field("vals", pa.list_(pa.int32()), nullable=True),
])
mid_list = pa.list_(inner_struct)
outer_struct = pa.struct([
    pa.field("items", mid_list, nullable=True),
])
root_type = pa.list_(outer_struct)

schema = pa.schema([
    pa.field("nested", root_type, nullable=True),
])

data = [
    None,
    [],
    [{"items": None}],
    [{"items": []}],
    [{"items": [{"vals": [1, 2]}, {"vals": None}]}],
    [{"items": [{"vals": [3]}, {"vals": [4, 5, 6]}]}, {"items": None}],
    None,
    [{"items": [{"vals": []}]}],
    [{"items": [{"vals": [10, 20]}, {"vals": [30]}]}, {"items": [{"vals": [40]}]}],
    [{"items": None}, {"items": [{"vals": [50, 60]}]}],
]

table = pa.table({"nested": data}, schema=schema)

path = os.path.join(DATA_DIR, "test_mixed_nested_5.lance")
if os.path.exists(path):
    os.remove(path)
with lance.file.LanceFileWriter(path, table.schema, version='2.1') as writer:
    writer.write_batch(table)
print(f"Created {path} with {table.num_rows} rows")
print("Schema:", table.schema)
