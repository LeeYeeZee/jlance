"""Milestone 57: 6-level nested list — list<list<list<list<list<list<int>>>>>>"""

import os
import pyarrow as pa
import lance

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "milestone_57")
os.makedirs(DATA_DIR, exist_ok=True)

# Build types from innermost outward
t1 = pa.list_(pa.int32())
t2 = pa.list_(t1)
t3 = pa.list_(t2)
t4 = pa.list_(t3)
t5 = pa.list_(t4)
t6 = pa.list_(t5)

# Test data
data = [
    None,                        # row 0: null
    [],                          # row 1: empty outer
    [[[[[[]]]]]],                # row 2: one empty chain
    [[[[[[1]]]]]],               # row 3: single value deep
    [[[[[[1, 2]], [[3, 4, 5]]]]]],  # row 4: two branches
    [[[[[[10]]]], [[[[20, 30]]]]]],  # row 5: two outers
    None,
    [],
    [[[[[[]]]]]],
    [[[[[[99]]]]]],
    [[[[[[11]], [[12, 13]]]]]],
    [[[[[[14]]]], [[[[15, 16, 17]]]]]],
    None,
    [],
    [[[[[[]]]]]],
    [[[[[[100, 200]]]]]],
    [[[[[[21]], [[22]]]]]],
    [[[[[[23]]]], [[[[24, 25]]]]]],
    None,
    [[[[[[26, 27, 28, 29, 30]]]]]],
]

schema = pa.schema([
    pa.field("deep", t6, nullable=True),
])

table = pa.table({"deep": pa.array(data, type=t6)}, schema=schema)

path = os.path.join(DATA_DIR, "test_nested_list_6.lance")
if os.path.exists(path):
    os.remove(path)
with lance.file.LanceFileWriter(path, table.schema, version='2.1') as writer:
    writer.write_batch(table)
print(f"Created {path} with {table.num_rows} rows")
print("Schema:", table.schema)
