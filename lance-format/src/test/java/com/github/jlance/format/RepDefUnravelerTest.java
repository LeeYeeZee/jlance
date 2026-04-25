// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.format;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import lance.encodings21.EncodingsV21.RepDefLayer;
import org.junit.jupiter.api.Test;

/**
 * Java port of Rust {@code lance-encoding/src/repdef.rs} unit tests.
 *
 * <p>These tests exercise {@link RepDefUnraveler} and {@link CompositeRepDefUnraveler}
 * directly with hand-crafted repetition / definition levels, mirroring the canonical
 * Rust test suite.
 */
class RepDefUnravelerTest {

  private static short[] s(int... values) {
    short[] result = new short[values.length];
    for (int i = 0; i < values.length; i++) {
      result[i] = (short) values[i];
    }
    return result;
  }

  private static List<RepDefLayer> layers(RepDefLayer... ls) {
    return List.of(ls);
  }

  // ---------------------------------------------------------------------------
  // 1. test_repdef_basic
  // ---------------------------------------------------------------------------
  @Test
  void testRepdefBasic() {
    short[] rep = s(2, 1, 0, 2, 2, 0, 1, 1, 0, 0, 0);
    short[] def = s(0, 0, 0, 3, 1, 1, 2, 1, 0, 0, 1);
    List<RepDefLayer> layers = layers(
        RepDefLayer.REPDEF_NULLABLE_ITEM,
        RepDefLayer.REPDEF_NULLABLE_LIST,
        RepDefLayer.REPDEF_NULLABLE_LIST);

    CompositeRepDefUnraveler unraveler = new CompositeRepDefUnraveler(
        List.of(new RepDefUnraveler(rep, def, layers, 9)));

    assertThat(unraveler.unravelValidity(9))
        .containsExactly(true, true, true, false, false, false, true, true, false);

    RepDefUnraveler.UnravelResult r1 = unraveler.unravelOffsets();
    assertThat(r1.offsets).containsExactly(0, 1, 3, 5, 5, 9);
    assertThat(r1.validity).containsExactly(true, true, true, false, true);

    RepDefUnraveler.UnravelResult r2 = unraveler.unravelOffsets();
    assertThat(r2.offsets).containsExactly(0, 2, 2, 5);
    assertThat(r2.validity).containsExactly(true, false, true);
  }

  // ---------------------------------------------------------------------------
  // 2. test_repdef_empty_list_no_null
  // ---------------------------------------------------------------------------
  @Test
  void testRepdefEmptyListNoNull() {
    short[] rep = s(1, 0, 0, 0, 1, 1, 1, 0);
    short[] def = s(0, 0, 0, 0, 1, 1, 0, 0);
    List<RepDefLayer> layers = layers(
        RepDefLayer.REPDEF_ALL_VALID_ITEM,
        RepDefLayer.REPDEF_EMPTYABLE_LIST);

    CompositeRepDefUnraveler unraveler = new CompositeRepDefUnraveler(
        List.of(new RepDefUnraveler(rep, def, layers, 8)));

    assertThat(unraveler.unravelValidity(6)).isNull();

    RepDefUnraveler.UnravelResult r1 = unraveler.unravelOffsets();
    assertThat(r1.offsets).containsExactly(0, 4, 4, 4, 6);
    assertThat(r1.validity).isNull();
  }

  // ---------------------------------------------------------------------------
  // 3. test_repdef_all_valid
  // ---------------------------------------------------------------------------
  @Test
  void testRepdefAllValid() {
    short[] rep = s(2, 1, 0, 2, 0, 2, 0, 1, 0);
    List<RepDefLayer> layers = layers(
        RepDefLayer.REPDEF_ALL_VALID_ITEM,
        RepDefLayer.REPDEF_ALL_VALID_LIST,
        RepDefLayer.REPDEF_ALL_VALID_LIST);

    CompositeRepDefUnraveler unraveler = new CompositeRepDefUnraveler(
        List.of(new RepDefUnraveler(rep, null, layers, 9)));

    assertThat(unraveler.unravelValidity(9)).isNull();

    RepDefUnraveler.UnravelResult r1 = unraveler.unravelOffsets();
    assertThat(r1.offsets).containsExactly(0, 1, 3, 5, 7, 9);
    assertThat(r1.validity).isNull();

    RepDefUnraveler.UnravelResult r2 = unraveler.unravelOffsets();
    assertThat(r2.offsets).containsExactly(0, 2, 3, 5);
    assertThat(r2.validity).isNull();
  }

  // ---------------------------------------------------------------------------
  // 4. test_only_empty_lists
  // ---------------------------------------------------------------------------
  @Test
  void testOnlyEmptyLists() {
    short[] rep = s(1, 0, 0, 0, 1, 1, 1, 0);
    short[] def = s(0, 0, 0, 0, 1, 1, 0, 0);
    List<RepDefLayer> layers = layers(
        RepDefLayer.REPDEF_ALL_VALID_ITEM,
        RepDefLayer.REPDEF_EMPTYABLE_LIST);

    CompositeRepDefUnraveler unraveler = new CompositeRepDefUnraveler(
        List.of(new RepDefUnraveler(rep, def, layers, 8)));

    assertThat(unraveler.unravelValidity(6)).isNull();

    RepDefUnraveler.UnravelResult r1 = unraveler.unravelOffsets();
    assertThat(r1.offsets).containsExactly(0, 4, 4, 4, 6);
    assertThat(r1.validity).isNull();
  }

  // ---------------------------------------------------------------------------
  // 5. test_only_null_lists
  // ---------------------------------------------------------------------------
  @Test
  void testOnlyNullLists() {
    short[] rep = s(1, 0, 0, 0, 1, 1, 1, 0);
    short[] def = s(0, 0, 0, 0, 1, 1, 0, 0);
    List<RepDefLayer> layers = layers(
        RepDefLayer.REPDEF_ALL_VALID_ITEM,
        RepDefLayer.REPDEF_NULLABLE_LIST);

    CompositeRepDefUnraveler unraveler = new CompositeRepDefUnraveler(
        List.of(new RepDefUnraveler(rep, def, layers, 8)));

    assertThat(unraveler.unravelValidity(6)).isNull();

    RepDefUnraveler.UnravelResult r1 = unraveler.unravelOffsets();
    assertThat(r1.offsets).containsExactly(0, 4, 4, 4, 6);
    assertThat(r1.validity).containsExactly(true, false, false, true);
  }

  // ---------------------------------------------------------------------------
  // 6. test_null_and_empty_lists
  // ---------------------------------------------------------------------------
  @Test
  void testNullAndEmptyLists() {
    short[] rep = s(1, 0, 0, 0, 1, 1, 1, 0);
    short[] def = s(0, 0, 0, 0, 1, 2, 0, 0);
    List<RepDefLayer> layers = layers(
        RepDefLayer.REPDEF_ALL_VALID_ITEM,
        RepDefLayer.REPDEF_NULL_AND_EMPTY_LIST);

    CompositeRepDefUnraveler unraveler = new CompositeRepDefUnraveler(
        List.of(new RepDefUnraveler(rep, def, layers, 8)));

    assertThat(unraveler.unravelValidity(6)).isNull();

    RepDefUnraveler.UnravelResult r1 = unraveler.unravelOffsets();
    assertThat(r1.offsets).containsExactly(0, 4, 4, 4, 6);
    assertThat(r1.validity).containsExactly(true, false, true, true);
  }

  // ---------------------------------------------------------------------------
  // 7. test_repdef_null_struct_valid_list
  // ---------------------------------------------------------------------------
  @Test
  void testRepdefNullStructValidList() {
    short[] rep = s(1, 0, 0, 0);
    short[] def = s(2, 0, 2, 2);
    List<RepDefLayer> layers = layers(
        RepDefLayer.REPDEF_NULLABLE_ITEM,
        RepDefLayer.REPDEF_NULLABLE_ITEM,
        RepDefLayer.REPDEF_ALL_VALID_LIST);

    CompositeRepDefUnraveler unraveler = new CompositeRepDefUnraveler(
        List.of(new RepDefUnraveler(rep, def, layers, 4)));

    assertThat(unraveler.unravelValidity(4))
        .containsExactly(false, true, false, false);
    assertThat(unraveler.unravelValidity(4))
        .containsExactly(false, true, false, false);

    RepDefUnraveler.UnravelResult r1 = unraveler.unravelOffsets();
    assertThat(r1.offsets).containsExactly(0, 4);
    assertThat(r1.validity).isNull();
  }

  // ---------------------------------------------------------------------------
  // 8. test_repdef_no_rep
  // ---------------------------------------------------------------------------
  @Test
  void testRepdefNoRep() {
    short[] def = s(2, 2, 0, 0, 1);
    List<RepDefLayer> layers = layers(
        RepDefLayer.REPDEF_NULLABLE_ITEM,
        RepDefLayer.REPDEF_NULLABLE_ITEM,
        RepDefLayer.REPDEF_ALL_VALID_ITEM);

    CompositeRepDefUnraveler unraveler = new CompositeRepDefUnraveler(
        List.of(new RepDefUnraveler(null, def, layers, 5)));

    assertThat(unraveler.unravelValidity(5))
        .containsExactly(false, false, true, true, false);
    assertThat(unraveler.unravelValidity(5))
        .containsExactly(false, false, true, true, true);
    assertThat(unraveler.unravelValidity(5)).isNull();
  }

  // ---------------------------------------------------------------------------
  // 9. regress_empty_list_case
  // ---------------------------------------------------------------------------
  @Test
  void regressEmptyListCase() {
    short[] rep = s(1, 1, 1);
    short[] def = s(1, 2, 1);
    // Inferred from Rust RepDefBuilder serialization: add_validity_bitmap,
    // add_offsets, add_no_null → reversed → [AllValidItem, NullableList, NullableItem]
    List<RepDefLayer> layers = layers(
        RepDefLayer.REPDEF_ALL_VALID_ITEM,
        RepDefLayer.REPDEF_NULLABLE_LIST,
        RepDefLayer.REPDEF_NULLABLE_ITEM);

    CompositeRepDefUnraveler unraveler = new CompositeRepDefUnraveler(
        List.of(new RepDefUnraveler(rep, def, layers, 0)));

    assertThat(unraveler.unravelValidity(0)).isNull();

    RepDefUnraveler.UnravelResult r1 = unraveler.unravelOffsets();
    assertThat(r1.offsets).containsExactly(0, 0, 0, 0);
    assertThat(r1.validity).containsExactly(false, false, false);

    assertThat(unraveler.unravelValidity(3))
        .containsExactly(true, false, true);
  }

  // ---------------------------------------------------------------------------
  // 10. regress_list_ends_null_case
  // ---------------------------------------------------------------------------
  @Test
  void regressListEndsNullCase() {
    short[] rep = s(2, 2, 2);
    short[] def = s(0, 1, 2);
    List<RepDefLayer> layers = layers(
        RepDefLayer.REPDEF_ALL_VALID_ITEM,
        RepDefLayer.REPDEF_NULLABLE_LIST,
        RepDefLayer.REPDEF_NULLABLE_LIST);

    CompositeRepDefUnraveler unraveler = new CompositeRepDefUnraveler(
        List.of(new RepDefUnraveler(rep, def, layers, 1)));

    assertThat(unraveler.unravelValidity(1)).isNull();

    RepDefUnraveler.UnravelResult r1 = unraveler.unravelOffsets();
    assertThat(r1.offsets).containsExactly(0, 1, 1);
    assertThat(r1.validity).containsExactly(true, false);

    RepDefUnraveler.UnravelResult r2 = unraveler.unravelOffsets();
    assertThat(r2.offsets).containsExactly(0, 1, 2, 2);
    assertThat(r2.validity).containsExactly(true, true, false);
  }

  // ---------------------------------------------------------------------------
  // 11. test_composite_unravel
  // ---------------------------------------------------------------------------
  @Test
  void testCompositeUnravel() {
    short[] rep1 = s(1, 0, 1, 1, 0, 0);
    short[] def1 = s(0, 0, 1, 0, 0, 0);
    List<RepDefLayer> layers1 = layers(
        RepDefLayer.REPDEF_ALL_VALID_ITEM,
        RepDefLayer.REPDEF_NULLABLE_LIST);

    short[] rep2 = s(1, 1, 0, 1, 0, 1, 0, 1, 0);
    List<RepDefLayer> layers2 = layers(
        RepDefLayer.REPDEF_ALL_VALID_ITEM,
        RepDefLayer.REPDEF_ALL_VALID_LIST);

    CompositeRepDefUnraveler unraveler = new CompositeRepDefUnraveler(List.of(
        new RepDefUnraveler(rep1, def1, layers1, 6),
        new RepDefUnraveler(rep2, null, layers2, 9)));

    assertThat(unraveler.unravelValidity(9)).isNull();

    RepDefUnraveler.UnravelResult r1 = unraveler.unravelOffsets();
    assertThat(r1.offsets).containsExactly(0, 2, 2, 5, 6, 8, 10, 12, 14);
    assertThat(r1.validity).containsExactly(true, false, true, true, true, true, true, true);
  }

  // ---------------------------------------------------------------------------
  // 12. test_mixed_unraveler
  // ---------------------------------------------------------------------------
  @Test
  void testMixedUnraveler() {
    // Case 1: one layer of validity, no repetition
    RepDefUnraveler u1a = new RepDefUnraveler(null, s(0, 1, 0, 1),
        layers(RepDefLayer.REPDEF_NULLABLE_ITEM), 4);
    RepDefUnraveler u1b = new RepDefUnraveler(null, null,
        layers(RepDefLayer.REPDEF_ALL_VALID_ITEM), 4);

    CompositeRepDefUnraveler unraveler1 = new CompositeRepDefUnraveler(List.of(u1a, u1b));
    assertThat(unraveler1.unravelValidity(8))
        .containsExactly(true, false, true, false, true, true, true, true);

    // Case 2: two layers of validity and repetition
    RepDefUnraveler u2a = new RepDefUnraveler(s(1, 0, 1), s(0, 1, 2),
        layers(RepDefLayer.REPDEF_NULLABLE_ITEM, RepDefLayer.REPDEF_EMPTYABLE_LIST), 2);
    RepDefUnraveler u2b = new RepDefUnraveler(s(1, 1, 0), s(1, 0, 0),
        layers(RepDefLayer.REPDEF_ALL_VALID_ITEM, RepDefLayer.REPDEF_NULLABLE_LIST), 2);

    CompositeRepDefUnraveler unraveler2 = new CompositeRepDefUnraveler(List.of(u2a, u2b));
    assertThat(unraveler2.unravelValidity(4))
        .containsExactly(true, false, true, true);

    RepDefUnraveler.UnravelResult r1 = unraveler2.unravelOffsets();
    assertThat(r1.offsets).containsExactly(0, 2, 2, 2, 4);
    assertThat(r1.validity).containsExactly(true, true, false, true);
  }

  // ---------------------------------------------------------------------------
  // 13. test_mixed_unraveler_nullable_without_def_levels
  // ---------------------------------------------------------------------------
  @Test
  void testMixedUnravelerNullableWithoutDefLevels() {
    RepDefUnraveler u1 = new RepDefUnraveler(null, s(0, 1, 0, 1),
        layers(RepDefLayer.REPDEF_NULLABLE_ITEM), 4);
    RepDefUnraveler u2 = new RepDefUnraveler(null, null,
        layers(RepDefLayer.REPDEF_NULLABLE_ITEM), 4);

    CompositeRepDefUnraveler unraveler = new CompositeRepDefUnraveler(List.of(u1, u2));
    assertThat(unraveler.unravelValidity(8))
        .containsExactly(true, false, true, false, true, true, true, true);
  }

  // ---------------------------------------------------------------------------
  // 14. test_repdef_empty_list_at_end
  // ---------------------------------------------------------------------------
  @Test
  void testRepdefEmptyListAtEnd() {
    // Rust test only asserts serialization; these decode expectations
    // are derived from Java's actual behaviour.
    short[] rep = s(1, 0, 1, 0, 0, 1);
    short[] def = s(0, 0, 0, 1, 0, 2);
    List<RepDefLayer> layers = layers(
        RepDefLayer.REPDEF_NULLABLE_ITEM,
        RepDefLayer.REPDEF_EMPTYABLE_LIST);

    CompositeRepDefUnraveler unraveler = new CompositeRepDefUnraveler(
        List.of(new RepDefUnraveler(rep, def, layers, 6)));

    assertThat(unraveler.unravelValidity(4))
        .containsExactly(true, true, true, false, true);

    RepDefUnraveler.UnravelResult r1 = unraveler.unravelOffsets();
    assertThat(r1.offsets).containsExactly(0, 2, 5, 5);
    assertThat(r1.validity).isNull();
  }

  // ---------------------------------------------------------------------------
  // 15. test_repdef_abnormal_nulls
  // ---------------------------------------------------------------------------
  @Test
  void testRepdefAbnormalNulls() {
    // Rust only asserts serialization for this case.
    short[] rep = s(1, 0, 1, 1, 0, 0);
    short[] def = s(0, 0, 1, 0, 0, 0);
    List<RepDefLayer> layers = layers(
        RepDefLayer.REPDEF_ALL_VALID_ITEM,
        RepDefLayer.REPDEF_NULLABLE_LIST);

    CompositeRepDefUnraveler unraveler = new CompositeRepDefUnraveler(
        List.of(new RepDefUnraveler(rep, def, layers, 5)));

    assertThat(unraveler.unravelValidity(3)).isNull();

    RepDefUnraveler.UnravelResult r1 = unraveler.unravelOffsets();
    assertThat(r1.offsets).containsExactly(0, 2, 2, 5);
    assertThat(r1.validity).containsExactly(true, false, true);
  }

  // ---------------------------------------------------------------------------
  // 16. unravelConstantLayer tests
  // ---------------------------------------------------------------------------
  @Test
  void testUnravelConstantLayerAllValid() {
    List<RepDefLayer> layers = layers(RepDefLayer.REPDEF_ALL_VALID_LIST);
    RepDefUnraveler unraveler = new RepDefUnraveler(new short[0], new short[0], layers, 3);

    RepDefUnraveler.UnravelResult r = unraveler.unravelOffsets(3);
    assertThat(r.offsets).containsExactly(0, 1, 2, 3);
    assertThat(r.validity).isNull();
  }

  @Test
  void testUnravelConstantLayerNullable() {
    List<RepDefLayer> layers = layers(RepDefLayer.REPDEF_NULLABLE_LIST);
    RepDefUnraveler unraveler = new RepDefUnraveler(new short[0], new short[0], layers, 3);

    RepDefUnraveler.UnravelResult r = unraveler.unravelOffsets(3);
    assertThat(r.offsets).containsExactly(0, 0, 0, 0);
    assertThat(r.validity).containsExactly(false, false, false);
  }

  @Test
  void testUnravelConstantLayerEmptyable() {
    List<RepDefLayer> layers = layers(RepDefLayer.REPDEF_EMPTYABLE_LIST);
    RepDefUnraveler unraveler = new RepDefUnraveler(new short[0], new short[0], layers, 3);

    RepDefUnraveler.UnravelResult r = unraveler.unravelOffsets(3);
    assertThat(r.offsets).containsExactly(0, 0, 0, 0);
    assertThat(r.validity).isNull();
  }
}
