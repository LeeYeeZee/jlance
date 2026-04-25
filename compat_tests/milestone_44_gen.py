"""Milestone 44: Deeply nested list test data generator (3+ layers)."""

import os
import pyarrow as pa
import lance.dataset
import lance.file

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "milestone_44")
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
    # Test 1: list<list<list<int>>> (3 layers) with mixed data
    # Row 0: [[[1, 2], [3]]]          (outer=1 list, inner=2 lists, items=3)
    # Row 1: None                      (null outer)
    # Row 2: [[[]]]                    (outer=1, inner=1, items=0)
    # Row 3: [[[4]], [[5, 6], [7]]]    (outer=2, inners=3, items=4)
    schema1 = pa.schema([
        pa.field('val', pa.list_(pa.list_(pa.list_(pa.int32()))), nullable=True),
    ])
    table1 = pa.table({
        'val': [
            [[[1, 2], [3]]],
            None,
            [[[]]],
            [[[4]], [[5, 6], [7]]],
        ],
    }, schema=schema1)
    write_file("test_nested_3layer", table1)

    # Test 2: list<list<list<int>>> all-null outer
    schema2 = pa.schema([
        pa.field('val', pa.list_(pa.list_(pa.list_(pa.int32()))), nullable=True),
    ])
    table2 = pa.table({
        'val': [None, None, None],
    }, schema=schema2)
    write_file("test_nested_3layer_all_null", table2)

    # Test 3: list<list<list<int>>> with empty middle lists
    # Row 0: [[[]], [[]]] -> outer=2, each middle=1, each inner=0
    # Row 1: [[[]], [[1]]] -> outer=2, middle0 empty, middle1 has 1 item
    # Row 2: None
    schema3 = pa.schema([
        pa.field('val', pa.list_(pa.list_(pa.list_(pa.int32()))), nullable=True),
    ])
    table3 = pa.table({
        'val': [
            [[[]], [[]]],
            [[[]], [[1]]],
            None,
        ],
    }, schema=schema3)
    write_file("test_nested_3layer_empty_middle", table3)

    # Test 4: list<list<list<list<int>>>> (4 layers) simple pattern
    schema4 = pa.schema([
        pa.field('val', pa.list_(pa.list_(pa.list_(pa.list_(pa.int32())))), nullable=True),
    ])
    table4 = pa.table({
        'val': [
            [[[[1, 2]], [[3]]]],
            None,
            [[[[4]]]],
        ],
    }, schema=schema4)
    write_file("test_nested_4layer", table4)

    print("Milestone 44 test data generation complete.")


if __name__ == "__main__":
    main()
