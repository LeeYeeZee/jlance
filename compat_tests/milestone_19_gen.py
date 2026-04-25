"""Generate multi-version Lance dataset for Java compat tests."""
import os
import lance.dataset
import lance.file
import pyarrow as pa
import numpy as np

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "milestone_19")


def write(name, table):
    path = os.path.join(OUT, name)
    if os.path.exists(path):
        import shutil
        shutil.rmtree(path)
    ds = lance.dataset.write_dataset(table, path)
    return ds


N = 20
rng = np.random.default_rng(42)

ids = np.arange(N, dtype=np.int32)
names = [f"row_{i:03d}" for i in range(N)]
scores = rng.random(size=N).astype(np.float64)

table = pa.table({
    "id": pa.array(ids),
    "name": pa.array(names),
    "score": pa.array(scores),
})

# Version 1: 20 rows
ds = write("test_versions", table)
print("Version 1 rows:", ds.count_rows())

# Version 2: delete 5 rows
ds.delete("id IN (1, 3, 5, 7, 9)")
print("Version 2 rows:", ds.count_rows())

# Version 3: append 10 more rows
N2 = 10
ids2 = np.arange(N, N + N2, dtype=np.int32)
names2 = [f"appended_{i:03d}" for i in range(N, N + N2)]
scores2 = rng.random(size=N2).astype(np.float64)
table2 = pa.table({
    "id": pa.array(ids2),
    "name": pa.array(names2),
    "score": pa.array(scores2),
})
ds = lance.dataset.write_dataset(table2, os.path.join(OUT, "test_versions"), mode="append")
print("Version 3 rows:", ds.count_rows())

print("Milestone 19 test data generated in", OUT)
