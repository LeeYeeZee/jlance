"""Generate Lance file for row-range reading compat tests."""
import os
import lance.dataset
import lance.file
import pyarrow as pa
import numpy as np

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "milestone_16")


def write(name, table):
    path = os.path.join(OUT, name)
    lance.dataset.write_dataset(table, path)


N = 100
rng = np.random.default_rng(42)

# Predictable data for easy verification
ids = np.arange(N, dtype=np.int32)
values = np.arange(N, dtype=np.float64) * 1.5
names = [f"row_{i:03d}" for i in range(N)]
scores = rng.random(size=N).astype(np.float32)

# Nullable column: every 5th row is null
nullable_vals = np.arange(N, dtype=np.int64)
nullable_arr = pa.array(
    [None if i % 5 == 0 else int(nullable_vals[i]) for i in range(N)]
)

table = pa.table({
    "id": pa.array(ids),
    "value": pa.array(values),
    "name": pa.array(names),
    "score": pa.array(scores),
    "nullable_int": nullable_arr,
})

write("test_row_range", table)

print("Milestone 16 test data generated in", OUT)
