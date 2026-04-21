"""Generate Lance V2.1 file for Milestone 37: Mixed primitives tests."""
import os
import pyarrow as pa
import lance
import numpy as np

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "milestone_37")
os.makedirs(DATA_DIR, exist_ok=True)

rng = np.random.default_rng(37)

N = 1000

# Mix of different numeric types
int64_col = rng.integers(-1000, 1000, size=N, dtype=np.int64)
int32_col = rng.integers(-500, 500, size=N, dtype=np.int32)
float32_col = rng.random(size=N, dtype=np.float32) * 100.0 - 50.0
float64_col = rng.random(size=N, dtype=np.float64) * 1000.0 - 500.0

# Nullable columns
nullable_int64 = [None if rng.random() < 0.2 else int(v) for v in rng.integers(0, 100, size=N)]
nullable_float64 = [None if rng.random() < 0.3 else float(v) for v in rng.random(size=N) * 10.0]

table = pa.table({
    "int64_col": pa.array(int64_col),
    "int32_col": pa.array(int32_col),
    "float32_col": pa.array(float32_col),
    "float64_col": pa.array(float64_col),
    "nullable_int64": pa.array(nullable_int64, type=pa.int64()),
    "nullable_float64": pa.array(nullable_float64, type=pa.float64()),
})

path = os.path.join(DATA_DIR, "test_mixed.lance")
if os.path.exists(path):
    os.remove(path)

with lance.file.LanceFileWriter(path, table.schema, version='2.1') as writer:
    writer.write_batch(table)

print(f"Created {path} with {table.num_rows} rows")
print("Schema:", table.schema)
