"""Generate Lance V2.1 file for Milestone 35: RLE pattern (highly repetitive) tests."""
import os
import pyarrow as pa
import lance.dataset
import lance.file
import numpy as np

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "milestone_35")
os.makedirs(DATA_DIR, exist_ok=True)

rng = np.random.default_rng(35)

N = 20000

# Highly repetitive int64: runs of same values
rle_int64 = []
val = 1
for i in range(N):
    rle_int64.append(val)
    if rng.random() < 0.02:  # Change value every ~50 rows
        val = rng.integers(0, 100)

# Highly repetitive float64
rle_float64 = []
fval = 1.5
for i in range(N):
    rle_float64.append(fval)
    if rng.random() < 0.03:
        fval = rng.random() * 100.0

# Boolean-like int64 (only 0 and 1)
rle_bool_like = [int(rng.random() < 0.5) for _ in range(N)]

table = pa.table({
    "rle_int64": pa.array(rle_int64, type=pa.int64()),
    "rle_float64": pa.array(rle_float64, type=pa.float64()),
    "rle_bool_like": pa.array(rle_bool_like, type=pa.int64()),
})

path = os.path.join(DATA_DIR, "test_rle.lance")
if os.path.exists(path):
    os.remove(path)

with lance.file.LanceFileWriter(path, table.schema, version='2.1') as writer:
    writer.write_batch(table)

print(f"Created {path} with {table.num_rows} rows")
print("Schema:", table.schema)
