package com.github.jlance.format.decoder;

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
  }

  /**
   * Creates a decoded array with no rep/def state (e.g. V2.0 columns or all-valid primitive).
   */
  public DecodedArray(FieldVector vector) {
    this(vector, null, null, null);
  }

  /**
   * Returns whether this decoded array carries any structural rep/def information.
   */
  public boolean hasRepDef() {
    return layers != null && !layers.isEmpty();
  }
}
