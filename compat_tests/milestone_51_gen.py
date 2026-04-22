"""Milestone 51: list<struct<list<string>>> test data generator."""

import os
import pyarrow as pa
import lance

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "milestone_51")
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
    inner_list_type = pa.list_(pa.string())
    struct_type = pa.struct([
        ("tags", inner_list_type),
        ("label", pa.string()),
    ])
    outer_list_type = pa.list_(struct_type)

    schema = pa.schema([
        pa.field("docs", outer_list_type, nullable=True),
    ])

    data = [
        # Row 0: 2 structs
        [{"tags": ["a", "bb"], "label": "x"}, {"tags": ["ccc"], "label": "y"}],
        # Row 1: 1 struct with empty inner list
        [{"tags": [], "label": "empty_tags"}],
        # Row 2: null outer list
        None,
        # Row 3: empty outer list
        [],
        # Row 4: 1 struct with longer strings
        [{"tags": ["hello", "world", "foo"], "label": "z"}],
    ]

    table = pa.table({"docs": data}, schema=schema)
    write_file("test_list_struct_list_string", table)

    print("Milestone 51 test data generation complete.")


if __name__ == "__main__":
    main()
