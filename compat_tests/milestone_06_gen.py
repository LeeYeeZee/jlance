"""Generate Lance datasets for Milestone 6 List type tests."""
import os
import sys
import pyarrow as pa
import lance.file as lf

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "milestone_06")


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
    # Verify
    reader = lf.LanceFileReader(file_path)
    meta = reader.metadata()
    for i, col in enumerate(meta.columns):
        print(f"  Column {i}: num_pages={len(col.pages)}")
    return file_path


def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    # 1. List of int32
    write_file("test_list_int32", pa.table({
        "val": pa.array([[1, 2, 3], [4, 5], None, [], [6]], type=pa.list_(pa.int32())),
    }))

    # 2. List of string
    write_file("test_list_string", pa.table({
        "val": pa.array([["a", "bb"], ["ccc"], None, [], ["d", "ee", "fff"]], type=pa.list_(pa.string())),
    }))

    # 3. Multi-page list of int32 (large dataset)
    write_file("test_list_int32_multi_page", pa.table({
        "val": pa.array([[i, i+1] for i in range(100000)], type=pa.list_(pa.int32())),
    }))

    return 0


if __name__ == "__main__":
    sys.exit(main())
