"""Generate Lance V2.1 file for Milestone 34: Multi-page numeric tests."""
import os
import pyarrow as pa
import lance.dataset
import lance.file
import numpy as np

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "milestone_34")
os.makedirs(DATA_DIR, exist_ok=True)

rng = np.random.default_rng(34)

N = 50000  # Large enough to trigger multiple pages

# Multi-page int64
multi_int64 = rng.integers(-1000000000, 1000000000, size=N, dtype=np.int64)

# Multi-page float64
multi_float64 = rng.random(size=N, dtype=np.float64) * 1000000.0 - 500000.0

# Multi-page nullable int64 with 30% nulls
multi_nullable_int64 = []
for v in rng.integers(0, 1000, size=N, dtype=np.int64):
    if rng.random() < 0.3:
        multi_nullable_int64.append(None)
    else:
        multi_nullable_int64.append(int(v))

table = pa.table({
    "multi_int64": pa.array(multi_int64),
    "multi_float64": pa.array(multi_float64),
    "multi_nullable_int64": pa.array(multi_nullable_int64, type=pa.int64()),
})

path = os.path.join(DATA_DIR, "test_multi_page.lance")
if os.path.exists(path):
    os.remove(path)

with lance.file.LanceFileWriter(path, table.schema, version='2.1') as writer:
    writer.write_batch(table)

print(f"Created {path} with {table.num_rows} rows")
print("Schema:", table.schema)
