"""Generate Lance file for Milestone 30: Dictionary with small index types."""
import os
import lance.dataset
import lance.file
import pyarrow as pa

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "milestone_30")


def write(name, table):
    path = os.path.join(OUT, name)
    if os.path.exists(path):
        import shutil
        shutil.rmtree(path)
    ds = lance.dataset.write_dataset(table, path)
    ds.optimize.compact_files()
    ds = lance.dataset.LanceDataset(path)
    ds.cleanup_old_versions(older_than=0, delete_unverified=True)
    print(f"Created dataset: {path}")


def main():
    os.makedirs(OUT, exist_ok=True)

    # Dictionary with int8 index (strings)
    # Dictionary with int16 index (int32 values)
    table = pa.table({
        "dict_int8_str": pa.array(
            ["a", "b", "a", "c", None, "b"],
            type=pa.dictionary(pa.int8(), pa.string())
        ),
        "dict_int16_int": pa.array(
            [10, 20, 10, 30, None, 20],
            type=pa.dictionary(pa.int16(), pa.int32())
        ),
    })
    write("test_dict_small_index", table)


if __name__ == "__main__":
    main()
