"""Generate Lance V2.1 files for Milestone 47: List type tests."""
import os
import sys
import pyarrow as pa
import lance.dataset
import lance.file

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "milestone_47")
os.makedirs(DATA_DIR, exist_ok=True)


def write_file(name, table):
    path = os.path.join(DATA_DIR, name + ".lance")
    if os.path.exists(path):
        os.remove(path)
    with lance.file.LanceFileWriter(path, table.schema, version='2.1') as writer:
        writer.write_batch(table)
    print(f"Created {path} with {table.num_rows} rows")
    print("Schema:", table.schema)
    return path


def main():
    # 1. Basic list of int32
    write_file("test_list_v21_basic", pa.table({
        "val": pa.array([[1, 2, 3], [4, 5], None, [], [6]], type=pa.list_(pa.int32())),
    }))

    # 2. List of string
    write_file("test_list_v21_string", pa.table({
        "val": pa.array([["a", "bb"], ["ccc"], None, [], ["d", "ee", "fff"]], type=pa.list_(pa.string())),
    }))

    # 3. Nullable list (list itself nullable, items not nullable)
    write_file("test_list_v21_nullable", pa.table({
        "val": pa.array([[1, 2], None, [3, 4, 5], [], [6]], type=pa.list_(pa.int32())),
    }))

    # 4. List with nullable items
    write_file("test_list_v21_nullable_items", pa.table({
        "val": pa.array([[1, None, 3], [None, 5], [6], [], [7, 8]], type=pa.list_(pa.int32())),
    }))

    # 5. LargeList of int64
    write_file("test_largelist_v21_basic", pa.table({
        "val": pa.array([[1, 2, 3], [4, 5], None, [], [6]], type=pa.large_list(pa.int64())),
    }))

    # 6. Multi-page list
    write_file("test_list_v21_multi_page", pa.table({
        "val": pa.array([[i, i+1] for i in range(100000)], type=pa.list_(pa.int32())),
    }))

    return 0


if __name__ == "__main__":
    sys.exit(main())
