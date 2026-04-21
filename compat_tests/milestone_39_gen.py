"""Generate Lance V2.1 dataset for InlineBitpacking / OutOfLineBitpacking tests."""
import os
import json
import lance
import pyarrow as pa
import numpy as np

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "milestone_39")
os.makedirs(OUT, exist_ok=True)

# Small-range int32 values should trigger InlineBitpacking in V2.1
N = 2050  # > 2 chunks (2 * 1024 = 2048) to test multi-chunk
rng = np.random.default_rng(42)

# Values 0..15 fit in 4 bits, triggering InlineBitpacking
small_values = rng.integers(0, 16, size=N, dtype=np.int32)

# Nullable small values: some entries are null (every 10th + some random)
small_nullable_raw = rng.integers(0, 8, size=N, dtype=np.int32)
small_nullable = [None if i % 10 == 0 else int(v) for i, v in enumerate(small_nullable_raw)]

# Also test u8 (0..255 range, may trigger InlineBitpacking with bit_width <= 8)
u8_values = rng.integers(0, 256, size=N, dtype=np.uint8)

table = pa.table({
    "small_int": pa.array(small_values),
    "small_nullable": pa.array(small_nullable, type=pa.int32()),
    "u8_col": pa.array(u8_values),
})

path = os.path.join(OUT, "test_bitpacking.lance")
if os.path.exists(path):
    os.remove(path)

# Write with version='2.1' to enable compressive encodings
with lance.file.LanceFileWriter(path, table.schema, version='2.1') as writer:
    writer.write_batch(table)

print(f"Generated {path} with {N} rows")
print("Schema:", table.schema)

# Save expected values for Java compatibility test
expected = {
    "row_count": N,
    "small_int_first_20": small_values[:20].tolist(),
    "small_int_last_10": small_values[-10:].tolist(),
    "small_nullable_first_20": small_nullable[:20],
    "small_nullable_last_10": small_nullable[-10:],
    "u8_col_first_20": u8_values[:20].tolist(),
    "u8_col_last_10": u8_values[-10:].tolist(),
}

expected_path = os.path.join(OUT, "expected.json")
with open(expected_path, "w") as f:
    json.dump(expected, f, indent=2)

print(f"Saved expected values to {expected_path}")
