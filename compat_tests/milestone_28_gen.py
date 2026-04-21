"""Generate Lance file for Milestone 28: Struct with null field."""
import os
import pyarrow as pa
import lance

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "milestone_28")


def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    # Struct with a regular nullable int field and a null field
    table = pa.table({
        "s": pa.array([
            {"a": 1, "b": None},
            {"a": 2, "b": None},
            {"a": None, "b": None},
            {"a": 4, "b": None},
        ], type=pa.struct([
            pa.field("a", pa.int32(), nullable=True),
            pa.field("b", pa.null(), nullable=True),
        ])),
    })
    path = os.path.join(DATA_DIR, "test_struct_null.lance")
    if os.path.exists(path):
        os.remove(path)
    writer = lance.file.LanceFileWriter(path, table.schema)
    writer.write_batch(table)
    writer.close()
    print(f"Created: {path}")


if __name__ == "__main__":
    main()
