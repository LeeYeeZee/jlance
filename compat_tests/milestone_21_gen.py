"""Generate Lance dataset with schema evolution for Java compat tests."""
import os
import lance
import pyarrow as pa
import numpy as np

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "milestone_21")


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

# Version 1: id + name
table = pa.table({
    "id": pa.array(ids),
    "name": pa.array(names),
})
ds = write("test_schema_evolution", table)
print("Version 1 fields:", ds.schema.names)

# Version 2: add score column
ds.add_columns({"score": "cast(id as float) * 1.5"})
print("Version 2 fields:", ds.schema.names)

# Version 3: drop name column
ds.drop_columns(["name"])
print("Version 3 fields:", ds.schema.names)

print("Milestone 21 test data generated in", OUT)
