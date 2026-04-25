"""Generate Lance V2.1 file for Milestone 31: Int64 compat tests."""
import os
import pyarrow as pa
import lance.dataset
import lance.file
import numpy as np

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "milestone_31")
os.makedirs(DATA_DIR, exist_ok=True)

rng = np.random.default_rng(31)

N = 5000

# Non-nullable int64 with some boundary values near the start
int64_vals = []
for i in range(N):
    if i == 0:
        int64_vals.append(np.iinfo(np.int64).max)
    elif i == 1:
        int64_vals.append(np.iinfo(np.int64).min)
    elif i == 2:
        int64_vals.append(0)
    elif i == 3:
        int64_vals.append(-1)
    elif i == 4:
        int64_vals.append(1)
    else:
        int64_vals.append(int(rng.integers(-1000000, 1000000)))

# Nullable int64 with 20% nulls
nullable_int64 = []
for i in range(N):
    if rng.random() < 0.2:
        nullable_int64.append(None)
    else:
        nullable_int64.append(int(rng.integers(-1000000, 1000000)))

# Large int64 array (same length as others for simplicity, but wide range)
large_int64 = rng.integers(-10000000000, 10000000000, size=N, dtype=np.int64)

table = pa.table({
    "int64_col": pa.array(int64_vals, type=pa.int64()),
    "nullable_int64": pa.array(nullable_int64, type=pa.int64()),
    "large_int64": pa.array(large_int64),
})

path = os.path.join(DATA_DIR, "test_int64.lance")
if os.path.exists(path):
    os.remove(path)

with lance.file.LanceFileWriter(path, table.schema, version='2.1') as writer:
    writer.write_batch(table)

print(f"Created {path} with {table.num_rows} rows")
print("Schema:", table.schema)
