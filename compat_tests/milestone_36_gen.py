"""Generate Lance V2.1 file for Milestone 36: High-entropy compressed data tests."""
import os
import pyarrow as pa
import lance.dataset
import lance.file
import numpy as np

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "milestone_36")
os.makedirs(DATA_DIR, exist_ok=True)

rng = np.random.default_rng(36)

N = 15000

# High-entropy int64 (should trigger zstd compression in MiniBlockLayout)
high_entropy_int64 = rng.integers(np.iinfo(np.int64).min, np.iinfo(np.int64).max, size=N, dtype=np.int64)

# High-entropy float64
high_entropy_float64 = rng.standard_normal(size=N).astype(np.float64)

# High-entropy int32
high_entropy_int32 = rng.integers(np.iinfo(np.int32).min, np.iinfo(np.int32).max, size=N, dtype=np.int32)

table = pa.table({
    "high_entropy_int64": pa.array(high_entropy_int64),
    "high_entropy_float64": pa.array(high_entropy_float64),
    "high_entropy_int32": pa.array(high_entropy_int32),
})

path = os.path.join(DATA_DIR, "test_high_entropy.lance")
if os.path.exists(path):
    os.remove(path)

with lance.file.LanceFileWriter(path, table.schema, version='2.1') as writer:
    writer.write_batch(table)

print(f"Created {path} with {table.num_rows} rows")
print("Schema:", table.schema)
