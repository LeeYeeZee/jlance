"""Generate Lance datasets for Milestone 3 primitive/struct decoding tests."""
import os
import sys
import pyarrow as pa
import lance.dataset
import lance.file

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "milestone_03")

def write_dataset(name, table):
    ds_path = os.path.join(DATA_DIR, name)
    if os.path.exists(ds_path):
        import shutil
        shutil.rmtree(ds_path)
    lance.dataset.write_dataset(
        table,
        ds_path,
        mode="create",
        max_rows_per_file=100,
        max_rows_per_group=100,
        data_storage_version="stable",
    )
    print(f"Created: {ds_path}")
    return ds_path

def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    # 1. Single non-nullable int32
    write_dataset("test_int32", pa.table({
        "val": pa.array([1, 2, 3, 4, 5], type=pa.int32()),
    }))

    # 2. Single nullable float64
    write_dataset("test_float64_nullable", pa.table({
        "val": pa.array([1.1, None, 3.3, None, 5.5], type=pa.float64()),
    }))

    # 3. Simple struct
    write_dataset("test_struct", pa.table({
        "s": pa.array([
            {"x": 1, "y": 1.1},
            {"x": 2, "y": 2.2},
            {"x": 3, "y": None},
        ], type=pa.struct([("x", pa.int32()), ("y", pa.float32())])),
    }))

    return 0

if __name__ == "__main__":
    sys.exit(main())
