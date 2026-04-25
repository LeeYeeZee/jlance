"""Generate Lance files that may trigger Constant encoding for Java compat tests."""
import os
import lance.dataset
import lance.file
import pyarrow as pa

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "milestone_14")


def write(name, table):
    path = os.path.join(OUT, name)
    if os.path.exists(path):
        import shutil
        shutil.rmtree(path)
    ds = lance.dataset.write_dataset(table, path)
    ds.optimize.compact_files()
    ds = lance.dataset.LanceDataset(path)
    ds.cleanup_old_versions(older_than=0, delete_unverified=True)


# --- All-same int32 values (may trigger Constant encoding) ---
same_int = pa.array([42, 42, 42, 42, 42], type=pa.int32())
table1 = pa.table({"same_int": same_int})
write("test_constant_int32", table1)

# --- All-null column (may trigger Constant / AllNull encoding) ---
all_null = pa.array([None, None, None, None, None], type=pa.int32())
table2 = pa.table({"all_null_int": all_null})
write("test_all_null_int32", table2)

print("Milestone 14 test data generated in", OUT)
