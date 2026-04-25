"""Generate Lance files with Zstd compression for Java compat tests."""
import os
import lance.dataset
import lance.file
import pyarrow as pa
import numpy as np

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "milestone_12")


def write(name, table):
    path = os.path.join(OUT, name)
    if os.path.exists(path):
        import shutil
        shutil.rmtree(path)
    ds = lance.dataset.write_dataset(table, path)
    ds.optimize.compact_files()
    ds = lance.dataset.LanceDataset(path)
    ds.cleanup_old_versions(older_than=0, delete_unverified=True)


# --- Single-page Zstd compressed int32 + string ---
schema_single = pa.schema([
    pa.field("int32_col", pa.int32(), metadata={"lance-encoding:compression": "zstd"}),
    pa.field("str_col", pa.string(), metadata={"lance-encoding:compression": "zstd"}),
])
table_single = pa.table({
    "int32_col": [1, 2, 3, None, 5],
    "str_col": ["a", "bb", None, "dddd", "e"],
}, schema=schema_single)
write("test_zstd_single_page", table_single)

# --- Multi-page Zstd compressed struct ---
schema_struct = pa.schema([
    pa.field("s", pa.struct([
        pa.field("x", pa.int32(), metadata={"lance-encoding:compression": "zstd"}),
        pa.field("y", pa.string(), metadata={"lance-encoding:compression": "zstd"}),
    ]), metadata={"lance-encoding:compression": "zstd"}),
])
N = 100_000
rng = np.random.default_rng(42)
structs = [
    {"x": int(rng.integers(0, 1000)), "y": f"row_{i}"}
    for i in range(N)
]
table_struct = pa.table({
    "s": pa.array(structs, type=schema_struct.field("s").type),
}, schema=schema_struct)
write("test_zstd_multi_page_struct", table_struct)

print("Milestone 12 test data generated in", OUT)
