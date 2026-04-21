# JLance Design Document

> **Implementation Principle:** When adding new features, prefer referencing the Rust reference implementation directly (`lance-src/rust/lance-encoding/src/...`). Match its structure, naming, and algorithms where practical, translating idioms from Rust async/iterators to Java sequential code.

## Overview

JLance is a Java reader for the Lance columnar data format (V2.0 / V2.1). It reads `.lance` files and datasets, producing Apache Arrow `VectorSchemaRoot` objects. There is no writer implementation.

## Architecture

```
LanceFileReader        →  opens file, parses footer, reads metadata
  ├── LanceFileFooter  →  version, offsets, column metadata
  ├── LanceFileMetadata→  Arrow Schema + row count
  ├── LanceSchemaConverter → protobuf Field → Arrow Field/Schema
  └── PageDecoder      →  dispatches to ArrayDecoder implementations
        ├── FlatDecoder
        ├── NullableDecoder
        ├── BinaryDecoder
        ├── DictionaryDecoder
        ├── FixedSizeListDecoder
        ├── PackedStructDecoder      (V2.0 only)
        └── V21 Layout Decoders
              ├── MiniBlockLayoutDecoder
              ├── FullZipLayoutDecoder
              ├── ConstantLayoutDecoder
              └── StructuralListDecodeTask  (multi-layer lists)
```

## Milestone Feature Map

The compatibility test suite is organized by milestones of increasing complexity. Each milestone has a Python generator (`compat_tests/milestone_XX_gen.py`) that creates golden files using the official Python `lance` library, plus a Java test class that verifies the Java reader produces identical Arrow data.

### V2.0 Foundation (M01–M10)

| Milestone | Focus | Key Types / Features |
|-----------|-------|---------------------|
| **M01** | Footer parsing | V2.0 dataset footer, manifest, single fragment |
| **M02** | Primitive types | `int32`, `float64`, `string` |
| **M03** | Structs & nullable fields | `struct`, `float64 nullable`, `int32` |
| **M04** | Binary, bool, FSL, string | `binary`, `bool`, `fixed_size_list`, `string` |
| **M05** | Multi-page columns | `int32`, `string`, `struct` with `max_page_bytes=1024` |
| **M06** | List types | `list<int32>`, `list<string>`, multi-page lists |
| **M07** | Temporal types | `timestamp`, `timestamp` multi-page |
| **M08** | Decimal types | `decimal128`, `decimal128` multi-page |
| **M09** | Large types | `large_string` multi-page, `large_types` (large_binary, large_utf8) |
| **M10** | Dictionary encoding | `dictionary`, `dictionary` multi-page |

### V2.0 Advanced (M11–M24)

| Milestone | Focus | Key Types / Features |
|-----------|-------|---------------------|
| **M11** | Fixed-size binary & halffloat | `fixed_size_binary`, `halffloat`, `halffloat` multi-page |
| **M12** | Zstd compression | `zstd` single-page, `zstd` multi-page struct |
| **M13** | Complex nesting | `list<struct>`, `list<struct>` multi-page, `struct<list>` |
| **M14** | All-null & constant | `all_null_int32`, `constant_int32` |
| **M15** | Multi-fragment datasets | `multi_fragment` (5 fragments), `single_fragment` |
| **M16** | Row range reading | `readBatch` with row range subset |
| **M17** | Column projection | Read subset of columns |
| **M18** | Deletions | Dataset with deletion vectors |
| **M19** | Versions | Multi-version dataset, reading older versions |
| **M20** | Rowids | Row ID encoding, all-deleted, empty, multi-fragment with deletions, with deletions |
| **M21** | Schema evolution | Added columns across versions |
| **M22** | Count rows & empty dataset | `count_rows`, `empty` dataset |
| **M23** | Random access (`take`) | Take by index: single fragment, with deletions, cross-fragment, with projection, empty, out-of-bounds |
| **M24** | Head (`head`) | Top-N rows: single fragment, cross-fragment, with projection, empty dataset, zero |

### V2.1 Encoding Features (M25–M47)

| Milestone | Focus | Key Types / Features |
|-----------|-------|---------------------|
| **M25** | Null encoding | `null` values in V2.1 |
| **M26** | Dictionary (int) | `dict<int>` |
| **M27** | List of dictionary | `list<dict>` |
| **M28** | Struct with nulls | `struct` with nullable children in V2.1 |
| **M29** | Large list of struct | `large_list<struct>` |
| **M30** | Small index dictionary | `dict` with small integer index |
| **M31** | Int64 primitives | `int64` in V2.1 |
| **M32** | Float | `float` (float32) in V2.1 |
| **M33** | Constant encoding | `constant` values in V2.1 |
| **M34** | Multi-page V2.1 | Multi-page columns in V2.1 |
| **M35** | RLE | Run-length encoding in V2.1 |
| **M36** | High entropy | High-entropy data compression paths |
| **M37** | Mixed | Mixed data patterns |
| **M38** | Dictionary V2.1 | `dictionary` in V2.1 |
| **M39** | Bitpacking & OOL | Inline bitpacking, out-of-line (OOL) bitpacking |
| **M40** | BlobLayout (V2.2) | Large binary values (50KB–150KB) with empty and null rows, using `lance-encoding:blob` metadata |
| **M41** | Packed struct | `packed_struct` basic, `packed_struct` with FSL, `packed_struct` with mixed types |
| **M42** | Constant nested | `constant` nested values |
| **M43** | Nested lists V2.1 | Various edge cases: empty inner/outer, null inner/outer, mixed, all empty, all null, single empty, single row empty inner, complex, simple, two items each, two rows |
| **M44** | Deep nested lists | 3-layer `list<list<list<int>>>` and 4-layer `list<list<list<list<int>>>>` |
| **M46** | Struct V2.1 | `struct` basic, `struct` nullable, `struct` nullable children |
| **M47** | List V2.1 | `list` basic, multi-page, nullable, nullable items, string; `large_list` basic |
| **V21** | Comprehensive V2.1 | Primitives, nullable, bitpack, fullzip, high entropy, RLE, small data |

### Milestone Gaps

- **M45** — No milestone 45 exists (skipped).
- **Map type** — Not supported; this is a V2.2+ feature in the Rust reference.

## Repetition & Definition Level Handling

Lance V2.1 uses rep/def levels to encode nullability and nested list structure.

- **List decoding**: `V21ListUnraveler` (mirrors Rust `RepDefUnraveler`) consumes rep/def levels one list layer at a time, producing Arrow list offsets and validity bitmaps.
- **Struct decoding**: Currently reconstructs struct validity by peeking the first child's `MiniBlockLayout` definition levels. This is a fragile special-case; the Rust reference uses `CompositeRepDefUnraveler.unravel_validity()` for a general solution.
- **Layer ordering**: Layers are ordered **inner-to-outer** in the protobuf message. For `list<list<list<int>>>` the layers are `[ALL_VALID_ITEM, EMPTYABLE_LIST, ALL_VALID_LIST, NULLABLE_LIST]`.

## Known Limitations

1. **Reader only** — No writer implementation.
2. **V2.1 struct validity** — Only works when the first child uses `MiniBlockLayout`. `ConstantLayout` and `FullZipLayout` first children will produce incorrect struct nullability.
3. **FixedSizeList&lt;Struct&gt;** — Not supported in V2.1 (Rust routes this through `StructuralFixedSizeListDecoder`).
4. **Map** — Not supported (V2.2+ feature).
5. **Row-major scheduling** — Java decodes columns sequentially; Rust uses a pipelined row-major scheduler for better latency. This is a performance difference, not a correctness issue.
6. **Large files** — `readBatch()` reads all rows into memory at once. Maliciously crafted files with extreme buffer sizes could cause `OutOfMemoryError`.
