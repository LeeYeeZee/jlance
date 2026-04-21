"""Milestone 41: PackedStruct encoding (V2.0) test data generator."""

import os
import pyarrow as pa
import lance

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "milestone_41")


def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    # Test 1: Basic PackedStruct with int32 + float64
    schema1 = pa.schema([
        pa.field('packed_struct', pa.struct([
            pa.field('x', pa.int32()),
            pa.field('y', pa.float64()),
        ]), metadata={'packed': 'true'}),
    ])
    table1 = pa.table({
        'packed_struct': [
            {'x': i, 'y': float(i) * 1.5}
            for i in range(50)
        ],
    }, schema=schema1)
    path1 = os.path.join(DATA_DIR, "test_packed_struct_basic.lance")
    if os.path.exists(path1):
        os.remove(path1)
    writer1 = lance.file.LanceFileWriter(path1, table1.schema)
    writer1.write_batch(table1)
    writer1.close()
    print(f"Created: {path1}")

    # Test 2: PackedStruct with multiple fixed-width types
    schema2 = pa.schema([
        pa.field('packed', pa.struct([
            pa.field('a', pa.int8()),
            pa.field('b', pa.int16()),
            pa.field('c', pa.int32()),
            pa.field('d', pa.int64()),
            pa.field('e', pa.float32()),
            pa.field('f', pa.float64()),
        ]), metadata={'packed': 'true'}),
    ])
    table2 = pa.table({
        'packed': [
            {
                'a': (i % 128) - 64,
                'b': (i % 32768) - 16384,
                'c': i * 100,
                'd': i * 1000000,
                'e': float(i) * 0.5,
                'f': float(i) * 0.25,
            }
            for i in range(30)
        ],
    }, schema=schema2)
    path2 = os.path.join(DATA_DIR, "test_packed_struct_types.lance")
    if os.path.exists(path2):
        os.remove(path2)
    writer2 = lance.file.LanceFileWriter(path2, table2.schema)
    writer2.write_batch(table2)
    writer2.close()
    print(f"Created: {path2}")

    # Test 3: PackedStruct with FixedSizeList child
    schema3 = pa.schema([
        pa.field('packed', pa.struct([
            pa.field('id', pa.int32()),
            pa.field('coords', pa.list_(pa.float32(), 3)),
        ]), metadata={'packed': 'true'}),
    ])
    table3 = pa.table({
        'packed': [
            {
                'id': i,
                'coords': [float(i), float(i + 1), float(i + 2)],
            }
            for i in range(20)
        ],
    }, schema=schema3)
    path3 = os.path.join(DATA_DIR, "test_packed_struct_fsl.lance")
    if os.path.exists(path3):
        os.remove(path3)
    writer3 = lance.file.LanceFileWriter(path3, table3.schema)
    writer3.write_batch(table3)
    writer3.close()
    print(f"Created: {path3}")

    print("Milestone 41 test data generation complete.")


if __name__ == "__main__":
    main()
