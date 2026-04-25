"""Milestone 52: struct<list<int>, list<string>> test data generator."""

import os
import pyarrow as pa
import lance.dataset
import lance.file

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "milestone_52")
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
        ("numbers", pa.list_(pa.int32())),
        ("names", pa.list_(pa.string())),
    ])

    schema = pa.schema([
        pa.field("record", struct_type, nullable=True),
    ])

    data = [
        # Row 0: both lists have values
        {"numbers": [1, 2, 3], "names": ["a", "b"]},
        # Row 1: one list empty, one has values
        {"numbers": [], "names": ["c"]},
        # Row 2: null struct
        None,
        # Row 3: both lists empty
        {"numbers": [], "names": []},
        # Row 4: one list null, one has values
        {"numbers": None, "names": ["d", "e", "f"]},
    ]

    table = pa.table({"record": data}, schema=schema)
    write_file("test_struct_list_list", table)

    print("Milestone 52 test data generation complete.")


if __name__ == "__main__":
    main()
