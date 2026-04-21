"""Generate Lance datasets for Milestone 5 multi-page column tests."""
import os
import sys
import pyarrow as pa
import lance.file as lf
import numpy as np

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "milestone_05")


def write_file(name, table, max_page_bytes=1024):
    ds_path = os.path.join(DATA_DIR, name, "data")
    os.makedirs(ds_path, exist_ok=True)
    file_path = os.path.join(ds_path, "test.lance")
    if os.path.exists(file_path):
        os.remove(file_path)
    writer = lf.LanceFileWriter(file_path, schema=table.schema, max_page_bytes=max_page_bytes, version='stable')
    for batch in table.to_batches():
        writer.write_batch(batch)
    writer.close()
    print(f"Created: {file_path}")
    # Verify multi-page
    reader = lf.LanceFileReader(file_path)
    meta = reader.metadata()
    for i, col in enumerate(meta.columns):
        print(f"  Column {i}: num_pages={len(col.pages)}")
    return file_path


def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    # 1. Multi-page int32 (100000 rows, max_page_bytes=1024 -> many pages)
    write_file("test_int32_multi_page", pa.table({
        "val": pa.array(range(100000), type=pa.int32()),
    }))

    # 2. Multi-page string (10000 rows, max_page_bytes=1024 -> many pages)
    write_file("test_string_multi_page", pa.table({
        "val": pa.array([f"row_{i}" for i in range(10000)], type=pa.string()),
    }))

    # 3. Multi-page struct (100000 rows, max_page_bytes=1024 -> many pages)
    write_file("test_struct_multi_page", pa.table({
        "s": pa.array(
            [{"x": i, "y": float(i)} for i in range(100000)],
            type=pa.struct([("x", pa.int32()), ("y", pa.float32())])
        ),
    }))

    return 0


if __name__ == "__main__":
    sys.exit(main())
