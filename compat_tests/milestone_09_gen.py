"""Generate Lance files with large types for Java compat tests."""
import os
import lance
import pyarrow as pa

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "milestone_09")

def write(name, table):
    path = os.path.join(OUT, name)
    ds = lance.write_dataset(table, path)
    ds.optimize.compact_files()
    ds = lance.dataset(path)
    ds.cleanup_old_versions(older_than=0, delete_unverified=True)

# --- Single-page large types with nulls ---
large_str = pa.array([
    "hello",
    "world",
    None,
    "large string test",
    "",
], type=pa.large_utf8())

large_bin = pa.array([
    b"\x00\x01\x02",
    b"\xff\xfe",
    None,
    b"binary data here",
    b"",
], type=pa.large_binary())

large_list = pa.array([
    [1, 2, 3],
    [],
    None,
    [4, 5],
    [6, 7, 8, 9, 10],
], type=pa.large_list(pa.int32()))

table_single = pa.table({
    "large_str": large_str,
    "large_bin": large_bin,
    "large_list": large_list,
})
write("test_large_types", table_single)

# --- Multi-page large_string ---
N = 100_000
strings = [f"row_{i}_" + "x" * (i % 100) for i in range(N)]
large_str_multi = pa.array(strings, type=pa.large_utf8())

table_multi = pa.table({"large_str_multi": large_str_multi})
write("test_large_string_multi_page", table_multi)

print("Milestone 09 test data generated in", OUT)
