// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.format.decoder;

import com.github.jlance.format.buffer.PageBufferStore;
import java.util.List;
import lance.encodings21.EncodingsV21.ConstantLayout;
import lance.encodings21.EncodingsV21.PageLayout;
import lance.encodings21.EncodingsV21.RepDefLayer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Decodes a {@link ConstantLayout} into an Arrow vector.
 *
 * <p>ConstantLayout is used when all (visible) values in a page are the same scalar
 * value, or when all values are null.
 *
 * <p>Currently supports:
 * <ul>
 *   <li>No rep/def layers or a single {@code ALL_VALID_ITEM} layer</li>
 *   <li>Single {@code NULLABLE_ITEM} layer with 1-bit definition bitmap</li>
 * </ul>
 */
public class ConstantLayoutDecoder implements PageLayoutDecoder {

  @Override
  public DecodedArray decodeWithRepDef(
      PageLayout layout,
      int numRows,
      PageBufferStore store,
      Field field,
      BufferAllocator allocator) {
    var constantLayout = layout.getConstantLayout();

    List<RepDefLayer> layers = constantLayout.getLayersList();
    if (!isSupportedLayer(layers)) {
      throw new UnsupportedOperationException(
          "ConstantLayout with unsupported rep/def layers: " + layers);
    }

    boolean hasNullableItem = layers.stream()
        .anyMatch(l -> l == RepDefLayer.REPDEF_NULLABLE_ITEM);

    // Read repetition levels if present
    short[] repLevels = null;
    boolean hasRep = constantLayout.hasRepCompression()
        || constantLayout.getNumRepValues() > 0;
    if (hasRep) {
      byte[] repBuffer = RepDefUtils.readDefBuffer(
          store,
          constantLayout.hasRepCompression() ? constantLayout.getRepCompression() : null,
          (int) constantLayout.getNumRepValues(),
          allocator);
      int numRep = repBuffer.length / 2;
      repLevels = new short[numRep];
      java.nio.ByteBuffer.wrap(repBuffer)
          .order(java.nio.ByteOrder.LITTLE_ENDIAN)
          .asShortBuffer()
          .get(repLevels);
    }

    // Read definition levels if present
    boolean[] validity = null;
    short[] defLevels = null;
    if (hasNullableItem
        && (constantLayout.hasDefCompression() || constantLayout.getNumDefValues() > 0)) {
      byte[] defBuffer =
          RepDefUtils.readDefBuffer(
              store,
              constantLayout.hasDefCompression() ? constantLayout.getDefCompression() : null,
              (int) constantLayout.getNumDefValues(),
              allocator);
      validity = RepDefUtils.decodeDefBitmap(defBuffer, numRows);
      defLevels = new short[numRows];
      for (int i = 0; i < numRows; i++) {
        defLevels[i] = validity[i] ? (short) 0 : (short) 1;
      }
    }

    FieldVector vector;
    if (!constantLayout.hasInlineValue()) {
      vector = decodeAllNulls(numRows, field, allocator);
    } else if (validity != null) {
      vector = decodeMixedNulls(constantLayout, numRows, field, allocator, validity);
    } else {
      vector = decodeAllSameValue(constantLayout, numRows, field, allocator);
    }
    return new DecodedArray(vector, repLevels, defLevels, layers);
  }

  private static boolean isSupportedLayer(List<RepDefLayer> layers) {
    if (layers.isEmpty()) {
      return true;
    }
    for (RepDefLayer layer : layers) {
      switch (layer) {
        case REPDEF_ALL_VALID_ITEM:
        case REPDEF_ALL_VALID_LIST:
        case REPDEF_NULLABLE_ITEM:
        case REPDEF_NULLABLE_LIST:
        case REPDEF_EMPTYABLE_LIST:
        case REPDEF_NULL_AND_EMPTY_LIST:
          continue;
        default:
          return false;
      }
    }
    return true;
  }

  private static FieldVector decodeAllSameValue(
      ConstantLayout layout, int numRows, Field field, BufferAllocator allocator) {
    byte[] valueBytes = layout.getInlineValue().toByteArray();

    FieldVector vector = field.createVector(allocator);
    vector.setInitialCapacity(numRows);
    vector.allocateNew();

    if (numRows == 0) {
      vector.setValueCount(0);
      return vector;
    }

    ConstantValueSetter.setValue(vector, valueBytes, 0);
    for (int i = 1; i < numRows; i++) {
      ConstantValueSetter.copyValue(vector, 0, i);
    }

    vector.setValueCount(numRows);
    return vector;
  }

  private static FieldVector decodeAllNulls(
      int numRows, Field field, BufferAllocator allocator) {
    FieldVector vector = field.createVector(allocator);
    vector.allocateNew();
    for (int i = 0; i < numRows; i++) {
      vector.setNull(i);
    }
    vector.setValueCount(numRows);
    return vector;
  }

  private static FieldVector decodeMixedNulls(
      ConstantLayout layout,
      int numRows,
      Field field,
      BufferAllocator allocator,
      boolean[] validity) {
    byte[] valueBytes = layout.getInlineValue().toByteArray();

    FieldVector vector = field.createVector(allocator);
    vector.setInitialCapacity(numRows);
    vector.allocateNew();

    if (numRows > 0) {
      ConstantValueSetter.setValue(vector, valueBytes, 0);
    }

    for (int i = 0; i < numRows; i++) {
      if (validity[i]) {
        if (i > 0) {
          ConstantValueSetter.copyValue(vector, 0, i);
        }
      } else {
        vector.setNull(i);
      }
    }

    vector.setValueCount(numRows);
    return vector;
  }

  /**
   * Extracts raw definition levels from a ConstantLayout page.
   *
   * <p>This is used by {@link com.github.jlance.format.LanceFileReader} to reconstruct
   * nullable struct validity in V2.1+ files.
   *
   * @return a {@code short[]} containing one definition level per row, or {@code null}
   *         if the layout has no nullable layer
   */
  public static short[] extractDefinitionLevels(
      PageLayout layout, int numRows, PageBufferStore store, BufferAllocator allocator) {
    var constantLayout = layout.getConstantLayout();
    List<RepDefLayer> layers = constantLayout.getLayersList();

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

    boolean hasDefData = constantLayout.hasDefCompression()
        || constantLayout.getNumDefValues() > 0;
    if (!hasDefData) {
      return new short[numRows];
    }

    byte[] defBuffer = RepDefUtils.readDefBuffer(
        store,
        constantLayout.hasDefCompression() ? constantLayout.getDefCompression() : null,
        (int) constantLayout.getNumDefValues(),
        allocator);
    boolean[] validity = RepDefUtils.decodeDefBitmap(defBuffer, numRows);

    short[] result = new short[numRows];
    for (int i = 0; i < numRows; i++) {
      result[i] = validity[i] ? (short) 0 : (short) 1;
    }
    return result;
  }
}
