"""Generate Lance files with halffloat and fixed_size_binary types for Java compat tests."""
import os
import lance.dataset
import lance.file
import pyarrow as pa
import numpy as np

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "milestone_11")

def write(name, table):
    path = os.path.join(OUT, name)
    ds = lance.dataset.write_dataset(table, path)
    ds.optimize.compact_files()
    ds = lance.dataset.LanceDataset(path)
    ds.cleanup_old_versions(older_than=0, delete_unverified=True)

# --- Single-page halffloat with nulls ---
halffloat_arr = pa.array(
    [0.0, 1.5, -2.5, None, 3.25],
    type=pa.float16()
)

table_halffloat = pa.table({"halffloat_col": halffloat_arr})
write("test_halffloat", table_halffloat)

# --- Multi-page halffloat ---
N = 100_000
rng = np.random.default_rng(42)
vals = rng.random(size=N).astype(np.float16)
halffloat_multi = pa.array(vals, type=pa.float16())

table_halffloat_multi = pa.table({"halffloat_multi": halffloat_multi})
write("test_halffloat_multi_page", table_halffloat_multi)

# --- Fixed-size binary ---
fsb_arr = pa.array(
    [b"abcd", b"efgh", b"ijkl", None, b"mnop"],
    type=pa.binary(4)
)

table_fsb = pa.table({"fsb_col": fsb_arr})
write("test_fixed_size_binary", table_fsb)

print("Milestone 11 test data generated in", OUT)
