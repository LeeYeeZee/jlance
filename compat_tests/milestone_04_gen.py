"""Generate Lance datasets for Milestone 4 extended type decoding tests."""
import os
import sys
import pyarrow as pa
import lance

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "milestone_04")


def write_dataset(name, table):
    ds_path = os.path.join(DATA_DIR, name)
    if os.path.exists(ds_path):
        import shutil
        shutil.rmtree(ds_path)
    lance.write_dataset(
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

    # 1. String column
    write_dataset("test_string", pa.table({
        "val": pa.array(["hello", "world", None, "lance"], type=pa.string()),
    }))

    # 2. Binary column
    write_dataset("test_binary", pa.table({
        "val": pa.array([b"\x00\x01", b"\x02\x03", None, b"\xff"], type=pa.binary()),
    }))

    # 3. Bool column
    write_dataset("test_bool", pa.table({
        "val": pa.array([True, False, None, True, False], type=pa.bool_()),
    }))

    # 4. FixedSizeList column
    write_dataset("test_fixed_size_list", pa.table({
        "val": pa.array([
            [1.0, 2.0, 3.0],
            [4.0, 5.0, 6.0],
            None,
            [7.0, 8.0, 9.0],
        ], type=pa.list_(pa.float64(), 3)),
    }))

    return 0


if __name__ == "__main__":
    sys.exit(main())
