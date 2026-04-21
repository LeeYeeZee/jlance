"""Generate Lance dataset with deletions for Java compat tests."""
import os
import lance
import pyarrow as pa
import numpy as np

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "milestone_18")


def write(name, table):
    path = os.path.join(OUT, name)
    if os.path.exists(path):
        import shutil
        shutil.rmtree(path)
    ds = lance.write_dataset(table, path)
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

ds = write("test_deletions", table)

# Delete some rows using SQL predicate
# Delete rows where id is odd and < 10
ds.delete("id IN (1, 3, 5, 7, 9)")

print("Milestone 18 test data generated in", OUT)
