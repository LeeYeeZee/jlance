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
  public FieldVector decode(
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

    if (constantLayout.hasRepCompression()) {
      throw new UnsupportedOperationException(
          "ConstantLayout with repetition levels not yet supported");
    }

    boolean hasNullableItem =
        layers.size() == 1 && layers.get(0) == RepDefLayer.REPDEF_NULLABLE_ITEM;

    // Read definition levels if present
    boolean[] validity = null;
    if (hasNullableItem
        && (constantLayout.hasDefCompression() || constantLayout.getNumDefValues() > 0)) {
      byte[] defBuffer =
          RepDefUtils.readDefBuffer(
              store,
              constantLayout.hasDefCompression() ? constantLayout.getDefCompression() : null,
              (int) constantLayout.getNumDefValues(),
              allocator);
      validity = RepDefUtils.decodeDefBitmap(defBuffer, numRows);
    }

    if (!constantLayout.hasInlineValue()) {
      return decodeAllNulls(numRows, field, allocator);
    }

    if (validity != null) {
      return decodeMixedNulls(constantLayout, numRows, field, allocator, validity);
    }

    return decodeAllSameValue(constantLayout, numRows, field, allocator);
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
}
