"""Generate Lance files with dictionary-encoded types for Java compat tests."""
import os
import lance
import pyarrow as pa

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "milestone_10")

def write(name, table):
    path = os.path.join(OUT, name)
    ds = lance.write_dataset(table, path)
    ds.optimize.compact_files()
    ds = lance.dataset(path)
    ds.cleanup_old_versions(older_than=0, delete_unverified=True)

# --- Dictionary-encoded string ---
dict_str = pa.array(
    ["a", "b", "a", "c", None, "b", "a"],
    type=pa.dictionary(pa.int32(), pa.utf8())
)

table_single = pa.table({"dict_str": dict_str})
write("test_dictionary", table_single)

# --- Multi-page dictionary ---
N = 100_000
categories = ["alpha", "beta", "gamma", "delta", "epsilon"]
dict_multi = pa.array(
    [categories[i % len(categories)] for i in range(N)],
    type=pa.dictionary(pa.int32(), pa.utf8())
)

table_multi = pa.table({"dict_multi": dict_multi})
write("test_dictionary_multi_page", table_multi)

print("Milestone 10 test data generated in", OUT)
