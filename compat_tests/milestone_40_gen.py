"""Generate Lance V2.2 BlobLayout file for Milestone 40: large binary / string compat tests."""
import os
import pyarrow as pa
import lance.dataset
import lance.file
import numpy as np

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "milestone_40")
os.makedirs(DATA_DIR, exist_ok=True)

rng = np.random.default_rng(40)

N = 20

# Large binary values (100KB each)
large_binary = []
for i in range(N):
    if i == 0:
        large_binary.append(b"")  # empty value
    elif i == 1:
        large_binary.append(None)  # null value
    else:
        size = int(rng.integers(50000, 150000))
        data = bytes(rng.integers(0, 256, size=size, dtype=np.uint8))
        large_binary.append(data)

# Create schema with blob metadata
blob_field = pa.field(
    "large_binary",
    pa.large_binary(),
    nullable=True,
    metadata={"lance-encoding:blob": "true"},
)
schema = pa.schema([blob_field])

table = pa.table({"large_binary": pa.array(large_binary, type=pa.large_binary())}, schema=schema)

path = os.path.join(DATA_DIR, "test_blob_large_binary.lance")
if os.path.exists(path):
    os.remove(path)

with lance.file.LanceFileWriter(path, table.schema, version="2.2") as writer:
    writer.write_batch(table)

print(f"Created {path} with {table.num_rows} rows")
print("Schema:", table.schema)

# Verify with Python reader (low-level to avoid blob read issues)
reader = lance.file.LanceFileReader(path)
metadata = reader.metadata()
print(f"Verified {os.path.basename(path)}: rows={metadata.num_rows}")
for col in metadata.columns:
    for page in col.pages:
        layout_str = str(page.encoding)
        has_blob = "BlobLayout" in layout_str
        print(f"  BlobLayout={has_blob}")
        break
    break
