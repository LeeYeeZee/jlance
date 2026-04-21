"""Generate Lance datasets for Milestone 22: Count Rows compat tests."""
import os
import lance
import pyarrow as pa
import numpy as np

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "milestone_22")


def write(name, table):
    path = os.path.join(OUT, name)
    if os.path.exists(path):
        import shutil
        shutil.rmtree(path)
    ds = lance.write_dataset(table, path)
    return ds


# Dataset 1: test_count_rows
N = 100
rng = np.random.default_rng(42)

ids = np.arange(N, dtype=np.int32)
names = [f"row_{i:03d}" for i in range(N)]
scores = rng.random(size=N).astype(np.float64)

table = pa.table({
    "id": pa.array(ids),
    "name": pa.array(names),
    "score": pa.array(scores),
})

# Version 1: 100 rows
ds = write("test_count_rows", table)
print("Version 1 rows:", ds.count_rows())

# Version 2: delete 30 rows (id < 30)
ds.delete("id < 30")
print("Version 2 rows:", ds.count_rows())

# Version 3: append 50 more rows
N2 = 50
ids2 = np.arange(N, N + N2, dtype=np.int32)
names2 = [f"appended_{i:03d}" for i in range(N, N + N2)]
scores2 = rng.random(size=N2).astype(np.float64)
table2 = pa.table({
    "id": pa.array(ids2),
    "name": pa.array(names2),
    "score": pa.array(scores2),
})
lance.write_dataset(table2, os.path.join(OUT, "test_count_rows"), mode="append")
print("Version 3 rows:", ds.count_rows())

# Dataset 2: empty dataset
empty_table = pa.table({
    "id": pa.array([], type=pa.int32()),
    "name": pa.array([], type=pa.string()),
})
write("test_empty", empty_table)
print("Empty dataset rows:", 0)

print("Milestone 22 test data generated in", OUT)
