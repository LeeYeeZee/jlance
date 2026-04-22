"""Milestone 55: list<struct<list<int>>> with nullable struct fields."""

import os
import pyarrow as pa
import lance

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "milestone_55")
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
        pa.field("items", pa.list_(pa.int32()), nullable=True),
        pa.field("name", pa.string(), nullable=True),
    ])
    outer_type = pa.list_(struct_type)

    schema = pa.schema([
        pa.field("nested", outer_type, nullable=True),
    ])

    data = [
        # Row 0: 2 structs
        [{"items": [1, 2], "name": "a"}, {"items": [3], "name": "b"}],
        # Row 1: 1 struct with null items list
        [{"items": None, "name": "null_items"}],
        # Row 2: null outer
        None,
        # Row 3: empty outer
        [],
        # Row 4: 1 struct with null name
        [{"items": [4, 5], "name": None}],
        # Row 5: 1 struct with both null
        [{"items": None, "name": None}],
    ]

    table = pa.table({"nested": data}, schema=schema)
    write_file("test_list_struct_list_nullable", table)

    print("Milestone 55 test data generation complete.")


if __name__ == "__main__":
    main()
