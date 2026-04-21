package com.github.jlance.format.decoder;

import com.github.jlance.format.buffer.PageBufferStore;
import lance.encodings.EncodingsV20.ArrayEncoding;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Decodes a {@link lance.encodings.EncodingsV20.Constant} encoding into an Arrow vector.
 *
 * <p>All rows in the page share the same scalar value.
 */
public class ConstantDecoder implements ArrayDecoder {

  @Override
  public FieldVector decode(
      ArrayEncoding encoding,
      int numRows,
      PageBufferStore store,
      Field field,
      BufferAllocator allocator) {
    var constant = encoding.getConstant();
    byte[] valueBytes = constant.getValue().toByteArray();

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
}
