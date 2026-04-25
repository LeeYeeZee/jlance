"""Generate Lance V2.1 file for Milestone 42: ConstantLayout nested lists."""
import os
import pyarrow as pa
import lance.dataset
import lance.file

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "milestone_42")
os.makedirs(DATA_DIR, exist_ok=True)

inner_type = pa.list_(pa.int32())
outer_type = pa.list_(inner_type)

# All-null nested list
all_null = pa.array([None] * 100, type=outer_type)

# All-empty nested list (outer row is empty)
all_empty = pa.array([[]] * 100, type=outer_type)

# Each outer row has one empty inner list
one_empty_inner = pa.array([ [[]] ] * 100, type=outer_type)

table = pa.table({
    "all_null": all_null,
    "all_empty": all_empty,
    "one_empty_inner": one_empty_inner,
})

path = os.path.join(DATA_DIR, "test_constant_nested.lance")
if os.path.exists(path):
    os.remove(path)

with lance.file.LanceFileWriter(path, table.schema, version='2.1') as writer:
    writer.write_batch(table)

print(f"Created {path} with {table.num_rows} rows")
print("Schema:", table.schema)
