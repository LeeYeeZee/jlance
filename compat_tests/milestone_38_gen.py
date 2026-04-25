"""Generate Lance V2.1 file for Milestone 38: MiniBlockLayout with Dictionary tests."""
import os
import pyarrow as pa
import lance.dataset
import lance.file

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "milestone_38")
os.makedirs(DATA_DIR, exist_ok=True)

# Dictionary-encoded string with int32 indices
dict_str = pa.array(
    ["alpha", "beta", "gamma", "alpha", "delta", None, "beta", "gamma", "alpha", "delta"],
    type=pa.dictionary(pa.int32(), pa.string())
)

# Dictionary-encoded int64 values with int32 indices
dict_int = pa.array(
    [100, 200, 100, 300, None, 200, 100, 300, 200, 100],
    type=pa.dictionary(pa.int32(), pa.int64())
)

# Dictionary with small cardinality (should trigger dictionary encoding)
dict_small = pa.array(
    ["x", "y", "x", "x", "y", "x", "y", "y", "x", "y"],
    type=pa.dictionary(pa.int32(), pa.string())
)

# Nullable dictionary
dict_nullable = pa.array(
    ["a", None, "b", "a", None, "c", "a", "b", None, "c"],
    type=pa.dictionary(pa.int32(), pa.string())
)

table = pa.table({
    "dict_str": dict_str,
    "dict_int": dict_int,
    "dict_small": dict_small,
    "dict_nullable": dict_nullable,
})

path = os.path.join(DATA_DIR, "test_dictionary.lance")
if os.path.exists(path):
    os.remove(path)

with lance.file.LanceFileWriter(path, table.schema, version='2.1') as writer:
    writer.write_batch(table)

print(f"Created {path} with {table.num_rows} rows")
print("Schema:", table.schema)
