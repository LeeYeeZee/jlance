"""Generate Lance file for Milestone 25: Null type compat tests."""
import os
import pyarrow as pa
import lance

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "milestone_25")


def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    # Test 1: Simple null column (all values are null)
    table1 = pa.table({
        "null_col": pa.array([None, None, None, None, None], type=pa.null()),
    })
    path1 = os.path.join(DATA_DIR, "test_null.lance")
    if os.path.exists(path1):
        os.remove(path1)
    writer = lance.file.LanceFileWriter(path1, table1.schema)
    writer.write_batch(table1)
    writer.close()
    print(f"Created: {path1}")

    # Test 2: Null column alongside regular columns
    table2 = pa.table({
        "id": pa.array([1, 2, 3, 4, 5], type=pa.int32()),
        "always_null": pa.array([None, None, None, None, None], type=pa.null()),
        "name": pa.array(["a", "b", "c", "d", "e"], type=pa.string()),
    })
    path2 = os.path.join(DATA_DIR, "test_null_mixed.lance")
    if os.path.exists(path2):
        os.remove(path2)
    writer = lance.file.LanceFileWriter(path2, table2.schema)
    writer.write_batch(table2)
    writer.close()
    print(f"Created: {path2}")


if __name__ == "__main__":
    main()
