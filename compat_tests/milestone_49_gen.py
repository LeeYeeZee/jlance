"""Generate Lance datasets for Milestone 49: FSST string compression."""
import os
import sys
import pyarrow as pa
import lance

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "milestone_49")


def write_file(name, table):
    os.makedirs(DATA_DIR, exist_ok=True)
    file_path = os.path.join(DATA_DIR, f"{name}.lance")
    if os.path.exists(file_path):
        os.remove(file_path)

    # Write as V2.1 file
    with lance.file.LanceFileWriter(file_path, table.schema, version="2.1") as writer:
        writer.write_batch(table)
    print(f"Created: {file_path}")
    return file_path


def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    # 1. Basic FSST strings (auto-selected: total size > 32KB, max_len >= 5)
    # Generate repetitive structured strings to trigger FSST
    base_strings = [
        "Customer#000000001",
        "Customer#000000002",
        "Customer#000000003",
        "Customer#000000004",
        "Customer#000000005",
        "Order#000000001",
        "Order#000000002",
        "Order#000000003",
        "Item#000000001",
        "Item#000000002",
    ]
    # Repeat enough to exceed 32KB total
    repeated = (base_strings * 500)[:5000]
    write_file("test_fsst_auto", pa.table({
        "name": pa.array(repeated, type=pa.utf8()),
    }))

    # 2. FSST with explicit metadata
    write_file("test_fsst_explicit", pa.table({
        "name": pa.array(repeated, type=pa.utf8()),
    }))

    # 3. FSST with nulls
    with_nulls = repeated[:100] + [None] * 20 + repeated[100:200]
    write_file("test_fsst_nullable", pa.table({
        "name": pa.array(with_nulls, type=pa.utf8()),
    }))

    # 4. FSST large binary
    binary_data = [s.encode("utf-8") for s in repeated]
    write_file("test_fsst_binary", pa.table({
        "data": pa.array(binary_data, type=pa.binary()),
    }))

    return 0


if __name__ == "__main__":
    sys.exit(main())
