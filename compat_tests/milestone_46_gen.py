"""Milestone 46: V2.1 Struct support test data generator."""

import os
import pyarrow as pa
import lance.dataset
import lance.file

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "milestone_46")


def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    # Test 1: Basic V2.1 struct with primitive children
    schema1 = pa.schema([
        pa.field('s', pa.struct([
            pa.field('x', pa.int32()),
            pa.field('y', pa.float64()),
        ])),
    ])
    table1 = pa.table({
        's': [
            {'x': i, 'y': float(i) * 1.5}
            for i in range(20)
        ],
    }, schema=schema1)
    path1 = os.path.join(DATA_DIR, "test_struct_v21_basic.lance")
    if os.path.exists(path1):
        os.remove(path1)
    writer1 = lance.file.LanceFileWriter(path1, table1.schema, version='2.1')
    writer1.write_batch(table1)
    writer1.close()
    print(f"Created: {path1}")

    # Test 2: Nullable V2.1 struct
    schema2 = pa.schema([
        pa.field('s', pa.struct([
            pa.field('x', pa.int32()),
            pa.field('y', pa.float64()),
        ]), nullable=True),
    ])
    table2 = pa.table({
        's': [
            {'x': 1, 'y': 1.5},
            None,
            {'x': 3, 'y': 3.5},
            None,
            {'x': 5, 'y': 5.5},
        ],
    }, schema=schema2)
    path2 = os.path.join(DATA_DIR, "test_struct_v21_nullable.lance")
    if os.path.exists(path2):
        os.remove(path2)
    writer2 = lance.file.LanceFileWriter(path2, table2.schema, version='2.1')
    writer2.write_batch(table2)
    writer2.close()
    print(f"Created: {path2}")

    # Test 3: V2.1 struct with nullable child fields
    schema3 = pa.schema([
        pa.field('s', pa.struct([
            pa.field('x', pa.int32(), nullable=True),
            pa.field('y', pa.float64(), nullable=True),
        ]), nullable=False),
    ])
    table3 = pa.table({
        's': [
            {'x': 1, 'y': 1.5},
            {'x': None, 'y': 2.5},
            {'x': 3, 'y': None},
            {'x': None, 'y': None},
            {'x': 5, 'y': 5.5},
        ],
    }, schema=schema3)
    path3 = os.path.join(DATA_DIR, "test_struct_v21_nullable_children.lance")
    if os.path.exists(path3):
        os.remove(path3)
    writer3 = lance.file.LanceFileWriter(path3, table3.schema, version='2.1')
    writer3.write_batch(table3)
    writer3.close()
    print(f"Created: {path3}")

    print("Milestone 46 test data generation complete.")


if __name__ == "__main__":
    main()
