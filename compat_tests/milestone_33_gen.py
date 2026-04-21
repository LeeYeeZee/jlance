"""Generate Lance V2.1 file for Milestone 33: ConstantLayout (all-same / all-null) tests."""
import os
import pyarrow as pa
import lance

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "milestone_33")
os.makedirs(DATA_DIR, exist_ok=True)

N = 10000

# All-same int64
same_int64 = pa.array([42] * N, type=pa.int64())

# All-same float64
same_float64 = pa.array([3.14159] * N, type=pa.float64())

# All-null int64
all_null_int64 = pa.array([None] * N, type=pa.int64())

# All-null float32
all_null_float32 = pa.array([None] * N, type=pa.float32())

# All-same int32
same_int32 = pa.array([99] * N, type=pa.int32())

# Normal mixed values for reference
mixed_normal = pa.array(list(range(N)), type=pa.int32())

table = pa.table({
    "same_int64": same_int64,
    "same_float64": same_float64,
    "all_null_int64": all_null_int64,
    "all_null_float32": all_null_float32,
    "same_int32": same_int32,
    "mixed_normal": mixed_normal,
})

path = os.path.join(DATA_DIR, "test_constant.lance")
if os.path.exists(path):
    os.remove(path)

with lance.file.LanceFileWriter(path, table.schema, version='2.1') as writer:
    writer.write_batch(table)

print(f"Created {path} with {table.num_rows} rows")
print("Schema:", table.schema)
