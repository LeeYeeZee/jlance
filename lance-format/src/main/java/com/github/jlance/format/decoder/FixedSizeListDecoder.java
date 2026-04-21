package com.github.jlance.format.decoder;

import com.github.jlance.format.buffer.PageBufferStore;
import lance.encodings.EncodingsV20.ArrayEncoding;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Decodes a {@link lance.encodings.EncodingsV20.FixedSizeList} encoding into an Arrow {@link
 * FixedSizeListVector}.
 */
public class FixedSizeListDecoder implements ArrayDecoder {

  private final ArrayDecoder itemDecoder;

  public FixedSizeListDecoder(ArrayDecoder itemDecoder) {
    this.itemDecoder = itemDecoder;
  }

  @Override
  public FieldVector decode(
      ArrayEncoding encoding,
      int numRows,
      PageBufferStore store,
      Field field,
      BufferAllocator allocator) {
    var fsl = encoding.getFixedSizeList();
    int dimension = (int) fsl.getDimension();

    FixedSizeListVector listVec = (FixedSizeListVector) field.createVector(allocator);
    listVec.setInitialCapacity(numRows);
    listVec.allocateNew();
    for (int i = 0; i < numRows; i++) {
      listVec.setNotNull(i);
    }

    Field childField = field.getChildren().get(0);
    FieldVector childVec =
        itemDecoder.decode(fsl.getItems(), numRows * dimension, store, childField, allocator);

    org.apache.arrow.vector.ValueVector slot =
        listVec.addOrGetVector(childField.getFieldType()).getVector();
    childVec.makeTransferPair(slot).transfer();
    childVec.close();

    listVec.setValueCount(numRows);
    return listVec;
  }
}
