"""Extra test data for Milestone 46: nullable struct with ConstantLayout first child."""

import os
import pyarrow as pa
import lance

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "milestone_46")
os.makedirs(DATA_DIR, exist_ok=True)

# Test: Nullable struct where first child has all-same values.
# This should cause Lance to use ConstantLayout for the first child,
# and the struct nullability must be recovered from ConstantLayout def levels.
schema = pa.schema([
    pa.field('s', pa.struct([
        pa.field('x', pa.int32()),
        pa.field('y', pa.float64()),
    ]), nullable=True),
])

# All non-null rows have x=42, y=3.14
table = pa.table({
    's': [
        {'x': 42, 'y': 3.14},
        None,
        {'x': 42, 'y': 3.14},
        None,
        {'x': 42, 'y': 3.14},
    ],
}, schema=schema)

path = os.path.join(DATA_DIR, "test_struct_v21_nullable_constant.lance")
if os.path.exists(path):
    os.remove(path)
writer = lance.file.LanceFileWriter(path, table.schema, version='2.1')
writer.write_batch(table)
writer.close()
print(f"Created: {path}")

# Verify with Python reader
reader = lance.file.LanceFileReader(path)
metadata = reader.metadata()
print(f"Verified: rows={metadata.num_rows}")

# Print page layouts to confirm ConstantLayout is used for first child
import json
for col_idx in range(metadata.num_columns):
    col_meta = metadata.column_metadata(col_idx)
    print(f"Column {col_idx}: {col_meta}")
