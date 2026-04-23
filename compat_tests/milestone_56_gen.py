"""Milestone 56: 5-level nested list — list<list<list<list<list<int>>>>>"""

import os
import pyarrow as pa
import lance

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "milestone_56")
os.makedirs(DATA_DIR, exist_ok=True)

# Build types from innermost outward
t1 = pa.list_(pa.int32())
t2 = pa.list_(t1)
t3 = pa.list_(t2)
t4 = pa.list_(t3)
t5 = pa.list_(t4)

# All-null
data_all_null = [None] * 20

# All-empty outer (each row is empty list at level 5)
data_all_empty = [[]] * 20

# One empty inner list at each depth level
data_mixed = [
    # row 0: null
    None,
    # row 1: empty outer
    [],
    # row 2: outer has one empty inner
    [[[[[]]]]],
    # row 3: outer has one inner with one empty, one inner with values
    [[[[[]], [[1, 2, 3]]]]],
    # row 4: more complex
    [[[[[10, 20]], [[30, 40, 50]]], [[[60]]]]],
    # row 5: multiple outers
    [[[[[1]]]], [[[[2, 3]], [[4, 5, 6]]]]],
    # rows 6-19: repeat pattern
    None, [], [[[[[]]]]], [[[[[7, 8]]]]], [[[[[9]]]], [[[[10, 11]]]]],
    None, [], [[[[[]]]]], [[[[[12]]]]], [[[[[13, 14]]]], [[[[15]]]]],
    None, [], [[[[[]]]]], [[[[[16]]]]],
]

schema = pa.schema([
    pa.field("all_null", t5, nullable=True),
    pa.field("all_empty", t5, nullable=True),
    pa.field("mixed", t5, nullable=True),
])

table = pa.table({
    "all_null": pa.array(data_all_null, type=t5),
    "all_empty": pa.array(data_all_empty, type=t5),
    "mixed": pa.array(data_mixed, type=t5),
}, schema=schema)

path = os.path.join(DATA_DIR, "test_nested_list_5.lance")
if os.path.exists(path):
    os.remove(path)
with lance.file.LanceFileWriter(path, table.schema, version='2.1') as writer:
    writer.write_batch(table)
print(f"Created {path} with {table.num_rows} rows")
print("Schema:", table.schema)
