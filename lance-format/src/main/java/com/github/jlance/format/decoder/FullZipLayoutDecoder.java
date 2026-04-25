// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.format.decoder;

import com.github.jlance.format.RepDefUnraveler;
import com.github.jlance.format.buffer.PageBufferStore;
import java.util.List;
import lance.encodings21.EncodingsV21.FullZipLayout;
import lance.encodings21.EncodingsV21.PageLayout;
import lance.encodings21.EncodingsV21.RepDefLayer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Decodes a {@link FullZipLayout} into an Arrow vector.
 *
 * <p>FullZipLayout is used for pages where the data is large. Values are stored in a
 * transposed (zipped) buffer layout with optional compression.
 *
 * <p>Currently supports:
 * <ul>
 *   <li>No repetition or definition levels, or a single {@code NULLABLE_ITEM} layer</li>
 *   <li>Fixed-width values ({@code bits_per_value})</li>
 *   <li>{@code Flat}, {@code General(zstd/lz4)}, or {@code Dictionary} value compression</li>
 * </ul>
 */
public class FullZipLayoutDecoder implements PageLayoutDecoder {

  @Override
  public DecodedArray decodeWithRepDef(
      PageLayout layout,
      int numRows,
      PageBufferStore store,
      Field field,
      BufferAllocator allocator) {
    var fullZip = layout.getFullZipLayout();

    List<RepDefLayer> layers = fullZip.getLayersList();
    if (!isSupportedLayer(layers)) {
      throw new UnsupportedOperationException(
          "FullZipLayout with unsupported rep/def layers: " + layers);
    }

    boolean hasNullableItem =
        layers.size() == 1 && layers.get(0) == RepDefLayer.REPDEF_NULLABLE_ITEM;

    // Repetition levels not yet supported
    if (fullZip.getBitsRep() > 0) {
      throw new UnsupportedOperationException(
          "FullZipLayout with repetition levels not yet supported");
    }

    // Variable width not yet supported
    if (fullZip.hasBitsPerOffset()) {
      throw new UnsupportedOperationException(
          "FullZipLayout with variable width (bits_per_offset) not yet supported");
    }

    // Read definition levels if present
    boolean[] validity = null;
    short[] defLevels = null;
    if (fullZip.getBitsDef() > 0) {
      if (fullZip.getBitsDef() != 1) {
        throw new UnsupportedOperationException(
            "FullZipLayout with bits_def != 1 not yet supported, got: " + fullZip.getBitsDef());
      }
      byte[] defBuffer = store.takeNextBuffer();
      validity = RepDefUtils.decodeDefBitmap(defBuffer, fullZip.getNumItems());
      defLevels = new short[numRows];
      for (int i = 0; i < numRows; i++) {
        defLevels[i] = validity[i] ? (short) 0 : (short) 1;
      }
    }

    // Decode values via compressive encoding tree
    FieldVector values = CompressiveEncodingDecoders.decodeToVector(
        fullZip.getValueCompression(), fullZip.getNumVisibleItems(), store, field, allocator);

    FieldVector vector;
    if (validity != null) {
      vector = FixedWidthVectorBuilder.expandWithValidity(
          values, validity, numRows, field, allocator);
    } else {
      vector = values;
    }
    RepDefUnraveler unraveler = new RepDefUnraveler(
        null, defLevels, layers, vector.getValueCount());
    if (defLevels != null) {
      unraveler.skipValidity();
    }
    return new DecodedArray(vector, unraveler);
  }

  private static boolean isSupportedLayer(List<RepDefLayer> layers) {
    if (layers.isEmpty()) {
      return true;
    }
    if (layers.size() == 1) {
      RepDefLayer layer = layers.get(0);
      return layer == RepDefLayer.REPDEF_ALL_VALID_ITEM
          || layer == RepDefLayer.REPDEF_NULLABLE_ITEM;
    }
    return false;
  }

  /**
   * Extracts raw definition levels from a FullZipLayout page.
   *
   * <p>This is used by {@link com.github.jlance.format.LanceFileReader} to reconstruct
   * nullable struct validity in V2.1+ files.
   *
   * @return a {@code short[]} containing one definition level per row, or {@code null}
   *         if the layout has no nullable layer
   */
  public static short[] extractDefinitionLevels(
      PageLayout layout, int numRows, PageBufferStore store, BufferAllocator allocator) {
    var fullZip = layout.getFullZipLayout();
    List<RepDefLayer> layers = fullZip.getLayersList();

    int nullableLayerIndex = -1;
    for (int li = layers.size() - 1; li >= 0; li--) {
      if (layers.get(li) == RepDefLayer.REPDEF_NULLABLE_ITEM) {
        nullableLayerIndex = li;
        break;
      }
    }
    if (nullableLayerIndex < 0) {
      return null;
    }

    if (fullZip.getBitsDef() == 0) {
      return new short[numRows];
    }

    byte[] defBuffer = store.getBuffer(store.getCurrentBufferIndex());
    boolean[] validity = RepDefUtils.decodeDefBitmap(defBuffer, fullZip.getNumItems());

    short[] result = new short[numRows];
    for (int i = 0; i < numRows; i++) {
      result[i] = validity[i] ? (short) 0 : (short) 1;
    }
    return result;
  }
}
