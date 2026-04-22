"""Milestone 54: large_list<struct<list<int>>> test data generator."""

import os
import pyarrow as pa
import lance

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "milestone_54")
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
        ("items", pa.list_(pa.int32())),
        ("name", pa.string()),
    ])
    outer_type = pa.large_list(struct_type)

    schema = pa.schema([
        pa.field("data", outer_type, nullable=True),
    ])

    data = [
        # Row 0: 2 structs
        [{"items": [1, 2], "name": "a"}, {"items": [3], "name": "b"}],
        # Row 1: 1 struct with empty inner list
        [{"items": [], "name": "c"}],
        # Row 2: null
        None,
        # Row 3: empty
        [],
        # Row 4: 1 struct
        [{"items": [4, 5, 6], "name": "d"}],
    ]

    table = pa.table({"data": data}, schema=schema)
    write_file("test_large_list_struct_list", table)

    print("Milestone 54 test data generation complete.")


if __name__ == "__main__":
    main()
