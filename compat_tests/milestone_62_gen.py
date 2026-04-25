"""Milestone 62: nullable struct with mixed child nullability test data generator."""

import os
import pyarrow as pa
import lance.file

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "milestone_62")
os.makedirs(DATA_DIR, exist_ok=True)


def write_file(name, table):
    path = os.path.join(DATA_DIR, name + ".lance")
    if os.path.exists(path):
        os.remove(path)
    with lance.file.LanceFileWriter(path, table.schema, version='2.1') as writer:
        writer.write_batch(table)
    print(f"Created {path} with {table.num_rows} rows")
    return path


def main():
    struct_type = pa.struct([
        ("a", pa.int32()),
        ("b", pa.utf8()),
    ])

    schema = pa.schema([
        pa.field("s", struct_type, nullable=True),
    ])

    # Row 0: struct present, a=1, b="x"
    # Row 1: struct present, a=null, b="y"
    # Row 2: struct present, a=2, b=null
    # Row 3: struct null
    # Row 4: struct present, a=null, b=null
    data = [
        {"a": 1, "b": "x"},
        {"a": None, "b": "y"},
        {"a": 2, "b": None},
        None,
        {"a": None, "b": None},
    ]

    table = pa.table({"s": data}, schema=schema)
    write_file("test_nullable_struct_mixed_nulls", table)

    print("Milestone 62 test data generation complete.")


if __name__ == "__main__":
    main()
