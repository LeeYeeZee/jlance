"""Generate Lance dataset with multiple fragments for Java compat tests."""
import os
import lance
import pyarrow as pa
import numpy as np

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "milestone_15")


def write(name, table, max_rows_per_file=None, max_rows_per_group=None):
    path = os.path.join(OUT, name)
    kwargs = {}
    if max_rows_per_file is not None:
        kwargs["max_rows_per_file"] = max_rows_per_file
    if max_rows_per_group is not None:
        kwargs["max_rows_per_group"] = max_rows_per_group
    ds = lance.write_dataset(table, path, **kwargs)
    return ds


# --- Multi-fragment dataset ---
N = 5000
rng = np.random.default_rng(42)
ints = rng.integers(0, 100000, size=N, dtype=np.int32)
floats = rng.random(size=N).astype(np.float64)

fragment_table = pa.table({
    "id": pa.array(ints),
    "value": pa.array(floats),
})

# Write with small max_rows_per_file to force multiple fragments
write("test_multi_fragment", fragment_table, max_rows_per_file=1000, max_rows_per_group=500)

# --- Single-fragment dataset (baseline) ---
single_table = pa.table({
    "name": pa.array(["alice", "bob", "charlie", "diana"]),
    "score": pa.array([10.5, 20.0, 15.3, 42.0]),
})
write("test_single_fragment", single_table)

print("Milestone 15 test data generated in", OUT)
