// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.format.decoder;

import com.github.jlance.format.RepDefUnraveler;
import java.util.Collections;
import java.util.List;
import lance.encodings21.EncodingsV21.RepDefLayer;
import org.apache.arrow.vector.FieldVector;

/**
 * The result of decoding a single V2.1+ page or column.
 *
 * <p>Mirrors the Rust {@code DecodedArray} struct.  Holds the decoded Arrow vector together
 * with the repetition/definition level state that was used to produce it.  The rep/def state
 * can be consumed by a parent {@link StructuralStructDecodeTask} to reconstruct struct
 * validity or by a list decoder to build list offsets.
 */
public class DecodedArray {

  public final FieldVector vector;
  public final short[] repLevels;
  public final short[] defLevels;
  public final List<RepDefLayer> layers;
  public final RepDefUnraveler unraveler;

  /**
   * Creates a decoded array with a pre-built rep/def unraveler.
   *
   * <p>This is the preferred constructor for V2.1+ structural decoding because the
   * unraveler carries mutable state (current layer, def cmp, rep cmp) that must be
   * preserved across nested list/struct boundaries.
   */
  public DecodedArray(FieldVector vector, RepDefUnraveler unraveler) {
    this.vector = vector;
    this.unraveler = unraveler;
    if (unraveler != null) {
      this.repLevels = unraveler.getRepLevels();
      this.defLevels = unraveler.getDefLevels();
      this.layers = unraveler.getLayers();
    } else {
      this.repLevels = null;
      this.defLevels = null;
      this.layers = null;
    }
  }

  /**
   * Creates a decoded array with rep/def state.
   *
   * @param vector   the decoded Arrow vector
   * @param repLevels repetition levels (may be {@code null} if no rep levels)
   * @param defLevels definition levels (may be {@code null} if no def levels)
   * @param layers    rep/def layer descriptors (inner-to-outer order)
   */
  public DecodedArray(
      FieldVector vector,
      short[] repLevels,
      short[] defLevels,
      List<RepDefLayer> layers) {
    this.vector = vector;
    this.repLevels = repLevels;
    this.defLevels = defLevels;
    this.layers = layers != null ? Collections.unmodifiableList(layers) : null;
    this.unraveler = (layers != null && !layers.isEmpty())
        ? new RepDefUnraveler(repLevels, defLevels, layers)
        : null;
  }

  /**
   * Creates a decoded array with no rep/def state (e.g. V2.0 columns or all-valid primitive).
   */
  public DecodedArray(FieldVector vector) {
    this.vector = vector;
    this.repLevels = null;
    this.defLevels = null;
    this.layers = null;
    this.unraveler = null;
  }

  /**
   * Returns whether this decoded array carries any structural rep/def information.
   */
  public boolean hasRepDef() {
    return (unraveler != null) || (layers != null && !layers.isEmpty());
  }
}
