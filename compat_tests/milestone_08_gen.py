"""Generate Lance files with decimal types for Java compat tests."""
import os
import lance
import pyarrow as pa
import numpy as np
from decimal import Decimal

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "milestone_08")

def write(name, table):
    path = os.path.join(OUT, name)
    ds = lance.write_dataset(table, path)
    ds.optimize.compact_files()
    ds = lance.dataset(path)  # re-open after compaction
    ds.cleanup_old_versions(older_than=0, delete_unverified=True)

# --- Single-page decimal128 and decimal256 with nulls ---
dec128 = pa.array([
    Decimal("0.00"),
    Decimal("123.45"),
    Decimal("-999.99"),
    None,
    Decimal("98765.43"),
], type=pa.decimal128(10, 2))

dec256 = pa.array([
    Decimal("0.000000"),
    Decimal("12345.678901"),
    Decimal("-999999999999.999999"),
    None,
    Decimal("12345678901234567890.123456"),
], type=pa.decimal256(38, 6))

table_single = pa.table({
    "dec128": dec128,
    "dec256": dec256,
})
write("test_decimal", table_single)

# --- Multi-page decimal128 ---
N = 100_000
rng = np.random.default_rng(42)
vals = rng.integers(-99999999, 99999999, size=N, dtype=np.int64)
multi_dec128 = pa.array(
    [Decimal(str(v / 10000.0)) for v in vals],
    type=pa.decimal128(18, 4)
)

table_multi = pa.table({"dec128_multi": multi_dec128})
write("test_decimal128_multi_page", table_multi)

print("Milestone 08 test data generated in", OUT)
