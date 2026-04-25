"""Milestone 59: 6-level mixed list/struct nesting."""

import os
import pyarrow as pa
import lance.dataset
import lance.file

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "milestone_59")
os.makedirs(DATA_DIR, exist_ok=True)

# Depth: list -> struct -> list -> struct -> list -> list -> int  (6 layers)
inner_inner = pa.list_(pa.int32())
inner_list = pa.list_(inner_inner)
inner_struct = pa.struct([
    pa.field("vals", inner_list, nullable=True),
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
    [{"items": [{"vals": [[1, 2], [3]]}, {"vals": None}]}],
    [{"items": [{"vals": [[4]]}, {"vals": [[5, 6], [7, 8, 9]]}]}, {"items": None}],
    None,
    [{"items": [{"vals": [[]]}]}],
    [{"items": [{"vals": [[10, 20]]}, {"vals": [[30]]}]}, {"items": [{"vals": [[40, 50]]}]}],
    [{"items": None}, {"items": [{"vals": [[60], [70, 80]]}]}],
    [{"items": [{"vals": [[90, 100, 110]]}]}],
    [{"items": [{"vals": None}, {"vals": [[120], [130]]}]}],
]

table = pa.table({"nested": data}, schema=schema)

path = os.path.join(DATA_DIR, "test_mixed_nested_6.lance")
if os.path.exists(path):
    os.remove(path)
with lance.file.LanceFileWriter(path, table.schema, version='2.1') as writer:
    writer.write_batch(table)
print(f"Created {path} with {table.num_rows} rows")
print("Schema:", table.schema)
