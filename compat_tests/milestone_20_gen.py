"""Generate Lance dataset for row ID testing."""
import os
import lance
import pyarrow as pa
import numpy as np

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "milestone_20")


def write(name, table):
    path = os.path.join(OUT, name)
    if os.path.exists(path):
        import shutil
        shutil.rmtree(path)
    ds = lance.write_dataset(table, path)
    return ds


rng = np.random.default_rng(42)
N = 1000

# --- test_rowids: two large fragments ---
ids0 = np.arange(N, dtype=np.int32)
names0 = [f"frag0_{i:05d}" for i in range(N)]
scores0 = rng.random(size=N).astype(np.float64)
table0 = pa.table({
    "id": pa.array(ids0),
    "name": pa.array(names0),
    "score": pa.array(scores0),
})

ds = write("test_rowids", table0)
print("Fragment 0 rows:", ds.count_rows())

ids1 = np.arange(N, 2 * N, dtype=np.int32)
names1 = [f"frag1_{i:05d}" for i in range(N, 2 * N)]
scores1 = rng.random(size=N).astype(np.float64)
table1 = pa.table({
    "id": pa.array(ids1),
    "name": pa.array(names1),
    "score": pa.array(scores1),
})
ds = lance.write_dataset(table1, os.path.join(OUT, "test_rowids"), mode="append")
print("Fragment 1 rows:", ds.count_rows())

# --- test_rowids_with_deletions: single fragment, delete 100 rows ---
ds_del = write("test_rowids_with_deletions", table0)
ds_del.delete("id >= 100 AND id < 200")
print("Deleted dataset rows:", ds_del.count_rows())

# --- test_rowids_multi_frag_with_deletions: 2 fragments, delete from first ---
ds_mfd = write("test_rowids_multi_frag_with_deletions", table0)
ds_mfd = lance.write_dataset(table1, os.path.join(OUT, "test_rowids_multi_frag_with_deletions"), mode="append")
ds_mfd.delete("id >= 100 AND id < 200")
print("Multi-frag deleted dataset rows:", ds_mfd.count_rows())

# --- test_rowids_all_deleted: single fragment, delete all rows ---
ds_all = write("test_rowids_all_deleted", table0)
ds_all.delete("id >= 0")
print("All-deleted dataset rows:", ds_all.count_rows())

# --- test_rowids_empty: empty dataset ---
empty_table = pa.table({
    "id": pa.array([], type=pa.int32()),
    "name": pa.array([], type=pa.string()),
    "score": pa.array([], type=pa.float64()),
})
write("test_rowids_empty", empty_table)
print("Empty dataset rows: 0")

print("Milestone 20 test data generated in", OUT)
