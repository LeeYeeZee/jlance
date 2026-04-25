"""Milestone 43: V2.1 MiniBlockLayout nested list test data generator."""

import os
import pyarrow as pa
import lance.dataset
import lance.file

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "milestone_43")


def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    schema = pa.schema([
        pa.field('mixed_nested', pa.list_(pa.list_(pa.int32())), nullable=True),
    ])

    data = []
    for i in range(100):
        if i % 4 == 0:
            data.append(None)  # null outer list
        elif i % 4 == 1:
            data.append([[]])  # outer list with one empty inner list
        elif i % 4 == 2:
            data.append([[1, 2, 3]])  # outer list with one inner list containing values
        else:
            data.append([[1], [2, 3]])  # outer list with two inner lists

    table = pa.table({'mixed_nested': data}, schema=schema)

    path = os.path.join(DATA_DIR, "test_miniblock_nested.lance")
    if os.path.exists(path):
        os.remove(path)
    writer = lance.file.LanceFileWriter(path, table.schema, version='2.1')
    writer.write_batch(table)
    writer.close()
    print(f"Created: {path}")

    # Verify
    reader = lance.file.LanceFileReader(path)
    print(f"Version: {reader.metadata().major_version}.{reader.metadata().minor_version}")
    print(f"Num rows: {reader.num_rows()}")
    for batch in reader.read_all().to_batches():
        print("First 5:", batch.column('mixed_nested').to_pylist()[:5])

    print("Milestone 43 test data generation complete.")


if __name__ == "__main__":
    main()
