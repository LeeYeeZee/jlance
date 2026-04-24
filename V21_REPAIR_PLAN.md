# V2.1 Nested Type Repair Plan — Align with Rust Lance

**Goal:** Fix all P0/P1/P2 issues from `V21_NESTED_TYPE_AUDIT.md` while aligning Java class/method names and structure with the canonical Rust implementation.

**Reference:** `lancedb/lance/rust/lance-encoding/src/repdef.rs`

---

## Phase 0 — Naming Alignment Table

Before touching logic, establish the name mapping so every future change follows it.

| Rust Name | Current Java Name | Target Java Name | File |
|-----------|-------------------|------------------|------|
| `RepDefUnraveler` | `V21ListUnraveler` | `RepDefUnraveler` | rename class |
| `unravel_offsets` | `unravelOffsets` | `unravelOffsets` | keep |
| `unravel_validity` | — | `unravelValidity` | new method |
| `levels_to_rep` | — | `levelsToRep` | new field (int[]) |
| `max_lists` | — | `maxLists` | new method |
| `current_def_cmp` | `currentDefCmp` | `currentDefCmp` | keep |
| `current_rep_cmp` | `currentRepCmp` | `currentRepCmp` | keep |
| `def_meaning` | `layers` (List<RepDefLayer>) | `defMeaning` | keep semantics, rename field if convenient |
| `CompositeRepDefUnraveler` | — | `CompositeRepDefUnraveler` | new class |
| `SerializedRepDefs` | — | `SerializedRepDefs` | new data class (optional) |
| `DefinitionInterpretation` | `RepDefLayer` (protobuf enum) | keep `RepDefLayer` | protobuf name is fine |

---

## Phase 1 — P0 Cleanup (no functional change, just hygiene)

**1.1 Remove disk-writing debug code**
- File: `LanceFileReader.java`
- Target: lines ~619-630 inside `decodeListColumn`
- Action: delete the `Files.write` block

**1.2 Remove `buildListVector` `System.out.println`**
- File: `LanceFileReader.java`
- Target: line ~1393
- Action: delete or replace with SLF4J debug

**1.3 Fix silent exception swallowing in `MiniBlockLayoutDecoder`**
- File: `MiniBlockLayoutDecoder.java`
- Target: lines 49-58 in `decodeWithRepDef`
- Action: remove try/catch, let exceptions propagate

---

## Phase 2 — P1 Structural Fixes (functional improvements, test-backed)

**2.1 Rename `V21ListUnraveler` → `RepDefUnraveler`**
- Rename class, update all imports/usages
- Keep `V21ListUnraveler` as a `@Deprecated` alias for backwards compat if needed

**2.2 Add `levelsToRep` field to `RepDefUnraveler`**
- Initialize in constructor exactly like Rust:
  ```rust
  let mut levels_to_rep = Vec::with_capacity(def_meaning.len());
  let mut rep_counter = 0;
  levels_to_rep.push(0);
  for meaning in def_meaning {
      match meaning {
          AllValidItem | AllValidList => {},
          NullableItem => levels_to_rep.push(rep_counter),
          NullableList => { rep_counter += 1; levels_to_rep.push(rep_counter); },
          EmptyableList => { rep_counter += 1; levels_to_rep.push(rep_counter); },
          NullableAndEmptyableList => {
              rep_counter += 1;
              levels_to_rep.push(rep_counter);
              levels_to_rep.push(rep_counter);
          }
      }
  }
  ```
- Use `levelsToRep` in `unravelOffsets` for the invisible-entry check instead of `maxLevel`

**2.3 Add `unravelValidity()` method**
- Mirrors Rust `RepDefUnraveler::unravel_validity`
- Skips remaining non-list layers
- If current layer is `NullableItem`, builds `boolean[]` (or `BooleanBuffer` concept) where `def == currentDefCmp` → valid, `def == currentDefCmp + 1` → null
- Returns `null` if all-valid

**2.4 Add `maxLists()` method**
- Mirrors Rust `RepDefUnraveler::max_lists`
- Returns `repLevels.length` if rep levels exist, else `0`

**2.5 Fix `hasConstantLayout` overwrite bug**
- Rename outer variable to `allPagesConstantLayout`
- Remove the inner-loop overwrite

**2.6 Fix `ConstantLayout` `NULL_AND_EMPTY_LIST` all-null assumption**
- Only assume all-null when `numRepValues == 0 && numDefValues == 0`
- Otherwise decode actual rep/def levels and interpret them

**2.7 Refactor single-layer list path in `decodeV21ListColumn`**
- Currently lines 1060–1175 have a hand-rolled rep/def walk
- Replace with `RepDefUnraveler` usage:
  1. Create `RepDefUnraveler`
  2. Call `unravelOffsets(numRows)` → get offsets + validity
  3. Call `unravelValidity()` → get item validity
  4. Build list vector from results
- This eliminates ~100 lines of duplicated logic and ensures consistency

---

## Phase 3 — P2 Architecture & Robustness

**3.1 Create `CompositeRepDefUnraveler`**
- New class that holds a list of `(repLevels, defLevels, pageRows)` tuples
- Provides `unravelOffsets(numRows)` that concatenates level buffers and re-runs `RepDefUnraveler` across the merged stream
- Provides `unravelValidity()` similarly
- Replace `mergeShortArrays` usage in multi-page paths

**3.2 Improve struct validity derivation**
- Current: first child only
- Target: intersection of all children (or dedicated `NullableItem` layer)
- Use `unravelValidity()` when available

**3.3 Add `StructuralListDecodeTask` as primary path**
- Rust uses a recursive task pattern
- Java already has `StructuralListDecodeTask` but it's secondary
- Make it the primary path, remove `buildMixedNestedVector` if it becomes redundant

**3.4 Add new compatibility tests**
- M59: `list<struct<list<struct<list<list<int>>>>>>`
- Struct with two list children
- `nullable struct<nullable int>`
- Multi-page V2.1 list (use `max_page_bytes=512` in Python generator)

---

## Execution Order

| Step | Phase | What | Estimated Files | Risk |
|------|-------|------|-----------------|------|
| 1 | 1.1 | Remove disk write | `LanceFileReader.java` | None |
| 2 | 1.2 | Remove debug print | `LanceFileReader.java` | None |
| 3 | 1.3 | Fix exception swallow | `MiniBlockLayoutDecoder.java` | Low (may expose existing hidden bugs) |
| 4 | 2.1 | Rename to `RepDefUnraveler` | 3-5 files | Low |
| 5 | 2.2 | Add `levelsToRep` | `RepDefUnraveler.java` | Medium (replaces core logic) |
| 6 | 2.3 | Add `unravelValidity` | `RepDefUnraveler.java` | Medium |
| 7 | 2.4 | Add `maxLists` | `RepDefUnraveler.java` | Low |
| 8 | 2.5 | Fix `hasConstantLayout` | `LanceFileReader.java` | Low |
| 9 | 2.6 | Fix `NULL_AND_EMPTY_LIST` | `ConstantLayoutDecoder.java` | Medium (needs test data) |
| 10 | 2.7 | Refactor single-layer path | `LanceFileReader.java` | High (most heavily used path) |
| 11 | 3.1 | `CompositeRepDefUnraveler` | new file + `LanceFileReader.java` | Medium |
| 12 | 3.2 | Struct validity intersection | `LanceFileReader.java` | Medium |
| 13 | 3.4 | New tests | Python + Java test files | Low |

---

**After every step:** run `mvn test -pl lance-tests-compat` to verify no regressions.
