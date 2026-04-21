"""Generate Lance V2.1 file for Milestone 32: Float32/Float64 compat tests."""
import os
import pyarrow as pa
import lance
import numpy as np

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "milestone_32")
os.makedirs(DATA_DIR, exist_ok=True)

rng = np.random.default_rng(32)

N = 3000

# Float32 with some special values near the start
float32_vals = []
for i in range(N):
    if i == 0:
        float32_vals.append(0.0)
    elif i == 1:
        float32_vals.append(-0.0)
    elif i == 2:
        float32_vals.append(float('inf'))
    elif i == 3:
        float32_vals.append(float('-inf'))
    elif i == 4:
        float32_vals.append(np.float32('nan'))
    else:
        float32_vals.append(float(rng.random() * 200.0 - 100.0))

# Float64 with some special values near the start
float64_vals = []
for i in range(N):
    if i == 0:
        float64_vals.append(0.0)
    elif i == 1:
        float64_vals.append(1.7976931348623157e+308)
    elif i == 2:
        float64_vals.append(-1.7976931348623157e+308)
    elif i == 3:
        float64_vals.append(2.2250738585072014e-308)
    elif i == 4:
        float64_vals.append(float('inf'))
    elif i == 5:
        float64_vals.append(float('-inf'))
    else:
        float64_vals.append(float(rng.random() * 1000000.0 - 500000.0))

# Nullable floats with 25% nulls
nullable_float32 = []
nullable_float64 = []
for i in range(N):
    if rng.random() < 0.25:
        nullable_float32.append(None)
        nullable_float64.append(None)
    else:
        nullable_float32.append(float(rng.random() * 100.0 - 50.0))
        nullable_float64.append(float(rng.random() * 1000.0 - 500.0))

table = pa.table({
    "float32_col": pa.array(float32_vals, type=pa.float32()),
    "float64_col": pa.array(float64_vals, type=pa.float64()),
    "nullable_float32": pa.array(nullable_float32, type=pa.float32()),
    "nullable_float64": pa.array(nullable_float64, type=pa.float64()),
})

path = os.path.join(DATA_DIR, "test_float.lance")
if os.path.exists(path):
    os.remove(path)

with lance.file.LanceFileWriter(path, table.schema, version='2.1') as writer:
    writer.write_batch(table)

print(f"Created {path} with {table.num_rows} rows")
print("Schema:", table.schema)
