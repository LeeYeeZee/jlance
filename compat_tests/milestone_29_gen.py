"""Generate Lance file for Milestone 29: LargeList of Struct."""
import os
import pyarrow as pa
import lance.dataset
import lance.file

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "milestone_29")


def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    # LargeList of struct
    table = pa.table({
        "large_list_struct": pa.array([
            [{"x": 1, "y": "a"}, {"x": 2, "y": "b"}],
            [{"x": 3, "y": "c"}],
            None,
            [{"x": 4, "y": "d"}, {"x": 5, "y": "e"}, {"x": 6, "y": "f"}],
        ], type=pa.large_list(pa.struct([
            pa.field("x", pa.int32(), nullable=False),
            pa.field("y", pa.string(), nullable=True),
        ]))),
    })
    path = os.path.join(DATA_DIR, "test_large_list_struct.lance")
    if os.path.exists(path):
        os.remove(path)
    writer = lance.file.LanceFileWriter(path, table.schema)
    writer.write_batch(table)
    writer.close()
    print(f"Created: {path}")


if __name__ == "__main__":
    main()
