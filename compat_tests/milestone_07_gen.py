"""Generate Lance datasets for Milestone 7 Temporal type tests."""
import os
import sys
import pyarrow as pa
import lance.file as lf

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "milestone_07")


def write_file(name, table, max_page_bytes=1024):
    ds_path = os.path.join(DATA_DIR, name, "data")
    os.makedirs(ds_path, exist_ok=True)
    file_path = os.path.join(ds_path, "test.lance")
    if os.path.exists(file_path):
        os.remove(file_path)
    writer = lf.LanceFileWriter(file_path, schema=table.schema, max_page_bytes=max_page_bytes, version='stable')
    for batch in table.to_batches():
        writer.write_batch(batch)
    writer.close()
    print(f"Created: {file_path}")
    return file_path


def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    # Single-page mixed temporal types
    write_file("test_temporal", pa.table({
        "ts_s": pa.array([0, 1, 2, None, 4], type=pa.timestamp('s')),
        "ts_ms": pa.array([0, 1000, 2000, None, 4000], type=pa.timestamp('ms')),
        "ts_us": pa.array([0, 1000000, 2000000, None, 4000000], type=pa.timestamp('us')),
        "ts_ns": pa.array([0, 1000000000, 2000000000, None, 4000000000], type=pa.timestamp('ns')),
        "date32": pa.array([0, 1, 2, None, 4], type=pa.date32()),
        "date64": pa.array([0, 86400000, 172800000, None, 345600000], type=pa.date64()),
        "time_s": pa.array([0, 1, 2, None, 4], type=pa.time32('s')),
        "time_ms": pa.array([0, 1000, 2000, None, 4000], type=pa.time32('ms')),
        "time_us": pa.array([0, 1000000, 2000000, None, 4000000], type=pa.time64('us')),
        "time_ns": pa.array([0, 1000000000, 2000000000, None, 4000000000], type=pa.time64('ns')),
        "duration_s": pa.array([0, 1, 2, None, 4], type=pa.duration('s')),
        "duration_ms": pa.array([0, 1000, 2000, None, 4000], type=pa.duration('ms')),
        "duration_us": pa.array([0, 1000000, 2000000, None, 4000000], type=pa.duration('us')),
        "duration_ns": pa.array([0, 1000000000, 2000000000, None, 4000000000], type=pa.duration('ns')),
    }))

    # Multi-page timestamp (us)
    write_file("test_timestamp_multi_page", pa.table({
        "ts_us": pa.array(list(range(100000)), type=pa.timestamp('us')),
    }))

    return 0


if __name__ == "__main__":
    sys.exit(main())
