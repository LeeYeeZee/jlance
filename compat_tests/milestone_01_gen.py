"""Generate a simple Lance dataset for Milestone 1 compatibility testing."""
import os
import sys
import pyarrow as pa
import lance.dataset
import lance.file

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "milestone_01")

def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    schema = pa.schema([
        pa.field("id", pa.int32(), nullable=False),
        pa.field("name", pa.utf8(), nullable=True),
    ])

    table = pa.table({
        "id": [1, 2, 3, 4, 5],
        "name": ["alice", "bob", "charlie", None, "eve"],
    }, schema=schema)

    ds_path = os.path.join(DATA_DIR, "test.lance")
    # Remove existing dataset to avoid append conflicts
    if os.path.exists(ds_path):
        import shutil
        shutil.rmtree(ds_path)

    lance.dataset.write_dataset(
        table,
        ds_path,
        mode="create",
        max_rows_per_file=100,  # keep single file for simple test
        max_rows_per_group=100,
        data_storage_version="stable",  # writes V2 files
    )

    print(f"Dataset created at: {ds_path}")
    ds = lance.dataset.LanceDataset(ds_path)
    print(f"Version: {ds.version}")
    print(f"Schema: {ds.schema}")
    print(f"Num rows: {ds.count_rows()}")
    print(f"Data files: {[f.path for frag in ds.get_fragments() for f in frag.data_files()]}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
