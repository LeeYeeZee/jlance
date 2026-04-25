"""Generate Lance dataset for column projection compat tests."""
import os
import lance.dataset
import lance.file
import pyarrow as pa
import numpy as np

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "milestone_17")


def write(name, table):
    path = os.path.join(OUT, name)
    lance.dataset.write_dataset(table, path)


N = 50
rng = np.random.default_rng(42)

ids = np.arange(N, dtype=np.int32)
names = [f"row_{i:03d}" for i in range(N)]
scores = rng.random(size=N).astype(np.float64)
flags = rng.integers(0, 2, size=N, dtype=np.bool_)

table = pa.table({
    "id": pa.array(ids),
    "name": pa.array(names),
    "score": pa.array(scores),
    "flag": pa.array(flags),
})

write("test_column_projection", table)

print("Milestone 17 test data generated in", OUT)
