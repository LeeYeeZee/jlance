"""Generate Lance file for Milestone 27: List of Dictionary values."""
import os
import pyarrow as pa
import lance

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "milestone_27")


def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    # List of dictionary-encoded strings
    table = pa.table({
        "list_dict": pa.array([
            ["a", "b"],
            ["c"],
            None,
            ["a", "c", "b"],
            ["d"],
        ], type=pa.list_(pa.dictionary(pa.int32(), pa.string()))),
    })
    path = os.path.join(DATA_DIR, "test_list_dict.lance")
    if os.path.exists(path):
        os.remove(path)
    writer = lance.file.LanceFileWriter(path, table.schema)
    writer.write_batch(table)
    writer.close()
    print(f"Created: {path}")


if __name__ == "__main__":
    main()
