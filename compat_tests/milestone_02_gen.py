"""Generate a Lance dataset with many primitive types for Milestone 2 testing."""
import os
import sys
import pyarrow as pa
import lance.dataset
import lance.file

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "milestone_02")

def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    schema = pa.schema([
        pa.field("int8_col", pa.int8(), nullable=False),
        pa.field("int16_col", pa.int16(), nullable=True),
        pa.field("int32_col", pa.int32(), nullable=False),
        pa.field("int64_col", pa.int64(), nullable=True),
        pa.field("uint8_col", pa.uint8(), nullable=False),
        pa.field("uint16_col", pa.uint16(), nullable=True),
        pa.field("uint32_col", pa.uint32(), nullable=False),
        pa.field("uint64_col", pa.uint64(), nullable=True),
        pa.field("float_col", pa.float32(), nullable=False),
        pa.field("double_col", pa.float64(), nullable=True),
        pa.field("bool_col", pa.bool_(), nullable=False),
        pa.field("string_col", pa.utf8(), nullable=True),
        pa.field("binary_col", pa.binary(), nullable=True),
    ])

    table = pa.table({
        "int8_col": [1, 2, 3],
        "int16_col": [10, None, 30],
        "int32_col": [100, 200, 300],
        "int64_col": [None, 2000, 3000],
        "uint8_col": [0, 1, 2],
        "uint16_col": [None, 100, 200],
        "uint32_col": [0, 1000, 2000],
        "uint64_col": [None, 10000, 20000],
        "float_col": [1.1, 2.2, 3.3],
        "double_col": [None, 2.22, 3.33],
        "bool_col": [True, False, True],
        "string_col": ["a", None, "c"],
        "binary_col": [b"\x01", None, b"\x03"],
    }, schema=schema)

    ds_path = os.path.join(DATA_DIR, "test.lance")
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

    print(f"Dataset created at: {ds_path}")
    ds = lance.dataset.LanceDataset(ds_path)
    print(f"Version: {ds.version}")
    print(f"Schema: {ds.schema}")
    print(f"Num rows: {ds.count_rows()}")

    # Print Lance logical types for debugging
    for frag in ds.get_fragments():
        for f in frag.data_files():
            print(f"File major: {f.file_major_version}, minor: {f.file_minor_version}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
