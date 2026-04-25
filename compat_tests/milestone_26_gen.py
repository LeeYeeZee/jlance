"""Generate Lance file for Milestone 26: Dictionary with non-string values."""
import os
import pyarrow as pa
import lance.dataset
import lance.file

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "milestone_26")


def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    # Dictionary-encoded int64 values
    table = pa.table({
        "dict_int": pa.array([100, 200, 100, 300, None, 200], type=pa.dictionary(pa.int32(), pa.int64())),
    })
    path = os.path.join(DATA_DIR, "test_dict_int.lance")
    if os.path.exists(path):
        os.remove(path)
    writer = lance.file.LanceFileWriter(path, table.schema)
    writer.write_batch(table)
    writer.close()
    print(f"Created: {path}")


if __name__ == "__main__":
    main()
