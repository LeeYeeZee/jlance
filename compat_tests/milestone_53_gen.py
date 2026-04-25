"""Milestone 53: list<struct<list<struct<int, string>>>> deeply nested test."""

import os
import pyarrow as pa
import lance.dataset
import lance.file

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "milestone_53")
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
    inner_struct = pa.struct([
        ("id", pa.int32()),
        ("desc", pa.string()),
    ])
    inner_list = pa.list_(inner_struct)
    outer_struct = pa.struct([
        ("items", inner_list),
        ("category", pa.string()),
    ])
    outer_list = pa.list_(outer_struct)

    schema = pa.schema([
        pa.field("entries", outer_list, nullable=True),
    ])

    data = [
        # Row 0: 2 outer structs
        [
            {"items": [{"id": 1, "desc": "a"}, {"id": 2, "desc": "b"}], "category": "cat1"},
            {"items": [{"id": 3, "desc": "c"}], "category": "cat2"},
        ],
        # Row 1: 1 outer struct with empty inner list
        [{"items": [], "category": "empty"}],
        # Row 2: null
        None,
        # Row 3: empty outer
        [],
        # Row 4: 1 outer struct, inner list with empty struct? no, empty list
        [{"items": [{"id": 4, "desc": "d"}, {"id": 5, "desc": "e"}, {"id": 6, "desc": "f"}], "category": "cat3"}],
    ]

    table = pa.table({"entries": data}, schema=schema)
    write_file("test_list_struct_list_struct", table)

    print("Milestone 53 test data generation complete.")


if __name__ == "__main__":
    main()
