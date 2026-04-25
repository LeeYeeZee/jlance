"""Milestone 48: FixedSizeList<Struct> V2.1 test data generator."""

import os
import pyarrow as pa
import lance.dataset
import lance.file

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "milestone_48")
os.makedirs(DATA_DIR, exist_ok=True)

schema = pa.schema([
    pa.field('fsl', pa.list_(
        pa.struct([
            pa.field('x', pa.int32()),
            pa.field('y', pa.float64()),
        ]),
        3
    ), nullable=True),
])

table = pa.table({
    'fsl': [
        [{'x': 1, 'y': 1.1}, {'x': 2, 'y': 2.2}, {'x': 3, 'y': 3.3}],
        None,
        [{'x': 4, 'y': 4.4}, {'x': 5, 'y': 5.5}, {'x': 6, 'y': 6.6}],
    ],
}, schema=schema)

path = os.path.join(DATA_DIR, "test_fsl_struct.lance")
if os.path.exists(path):
    os.remove(path)
writer = lance.file.LanceFileWriter(path, table.schema, version='2.1')
writer.write_batch(table)
writer.close()
print(f"Created: {path}")
print(f"Rows: {table.num_rows}")
