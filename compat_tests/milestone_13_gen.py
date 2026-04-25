"""Generate Lance files with complex nested types for Java compat tests."""
import os
import lance.dataset
import lance.file
import pyarrow as pa

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "milestone_13")


def write(name, table):
    path = os.path.join(OUT, name)
    if os.path.exists(path):
        import shutil
        shutil.rmtree(path)
    ds = lance.dataset.write_dataset(table, path)
    ds.optimize.compact_files()
    ds = lance.dataset.LanceDataset(path)
    ds.cleanup_old_versions(older_than=0, delete_unverified=True)


# --- List of Struct ---
list_of_struct = pa.array([
    [{"x": 1, "y": "a"}, {"x": 2, "y": "b"}],
    [{"x": 3, "y": "c"}],
    None,
    [],
    [{"x": 4, "y": "d"}, {"x": 5, "y": "e"}, {"x": 6, "y": "f"}],
], type=pa.list_(pa.struct([("x", pa.int32()), ("y", pa.string())])))

table1 = pa.table({"list_struct": list_of_struct})
write("test_list_of_struct", table1)

# --- Struct of List ---
struct_of_list = pa.array([
    {"items": [1, 2, 3], "name": "foo"},
    {"items": [], "name": "bar"},
    {"items": [4, 5], "name": None},
    {"items": None, "name": "baz"},
    {"items": [6], "name": "qux"},
], type=pa.struct([
    ("items", pa.list_(pa.int32())),
    ("name", pa.string()),
]))

table2 = pa.table({"struct_list": struct_of_list})
write("test_struct_of_list", table2)

# --- Multi-page List of Struct ---
N = 50_000
rng = __import__("numpy").random.default_rng(42)
large_list = []
for i in range(N):
    if i % 10 == 0:
        large_list.append(None)
    else:
        n_items = rng.integers(0, 5)
        large_list.append([{"x": int(j), "y": f"row_{i}_item_{j}"} for j in range(n_items)])

large_list_of_struct = pa.array(large_list, type=pa.list_(pa.struct([("x", pa.int32()), ("y", pa.string())])))
table3 = pa.table({"large_list_struct": large_list_of_struct})
write("test_list_of_struct_multi_page", table3)

print("Milestone 13 test data generated in", OUT)
