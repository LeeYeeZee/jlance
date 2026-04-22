// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.format.decoder;

import com.github.jlance.format.buffer.PageBufferStore;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import lance.encodings.EncodingsV20;
import lance.encodings.EncodingsV20.ArrayEncoding;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.complex.StructVector;

/**
 * Decodes a Lance V2.0 {@link EncodingsV20.PackedStruct} encoding into an Arrow {@link StructVector}.
 *
 * <p>PackedStruct stores all child fields of a struct in a single, row-major, fixed-width buffer.
 * Unlike {@code SimpleStruct} (where each child is stored as a separate column), PackedStruct
 * consumes exactly one column index. The child data is interleaved: for each row, the bytes of
 * field 0, field 1, ... field N are concatenated.
 *
 * <p>Restrictions (enforced by the Lance V2.0 writer):
 * <ul>
 *   <li>All child fields must be fixed-width (no strings, lists, or variable-size types)</li>
 *   <li>The struct itself may be nullable only if there are no actual null rows</li>
 * </ul>
 */
public class PackedStructDecoder implements ArrayDecoder {

  @Override
  public FieldVector decode(
      ArrayEncoding encoding,
      int numRows,
      PageBufferStore store,
      Field field,
      BufferAllocator allocator) {
    if (!encoding.hasPackedStruct()) {
      throw new IllegalArgumentException("Expected PackedStruct encoding, got: "
          + encoding.getArrayEncodingCase());
    }
    var packed = encoding.getPackedStruct();
    List<Field> children = field.getChildren();
    List<ArrayEncoding> inner = packed.getInnerList();
    if (children.size() != inner.size()) {
      throw new IllegalStateException(
          "PackedStruct child count mismatch: schema has " + children.size()
              + " children but encoding has " + inner.size());
    }

    // Read the single packed buffer
    int bufferIndex = packed.getBuffer().getBufferIndex();
    byte[] packedData = store.getBuffer(bufferIndex);

    // Compute stride (total bytes per row)
    int[] childBytes = new int[children.size()];
    int totalBytesPerRow = 0;
    for (int i = 0; i < children.size(); i++) {
      int bw = getByteWidth(children.get(i));
      childBytes[i] = bw;
      totalBytesPerRow += bw;
    }

    int expectedSize = numRows * totalBytesPerRow;
    if (packedData.length != expectedSize) {
      throw new IllegalStateException(
          "PackedStruct buffer size mismatch: expected " + expectedSize
              + " bytes for " + numRows + " rows * " + totalBytesPerRow
              + " bytes/row, got " + packedData.length);
    }

    // Extract each child's bytes and build its vector
    List<FieldVector> childVectors = new ArrayList<>(children.size());
    ByteBuffer packedBuf = ByteBuffer.wrap(packedData).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < children.size(); i++) {
      Field childField = children.get(i);
      int bw = childBytes[i];
      int childBits = bw * 8;

      // Gather this child's bytes by striding through the packed buffer
      byte[] childData = new byte[numRows * bw];
      int offsetInRow = 0;
      for (int j = 0; j < i; j++) {
        offsetInRow += childBytes[j];
      }
      for (int row = 0; row < numRows; row++) {
        int rowOffset = row * totalBytesPerRow + offsetInRow;
        packedBuf.position(rowOffset);
        packedBuf.get(childData, row * bw, bw);
      }

      FieldVector childVec = buildChildVector(
          childField, numRows, childData, childBits, allocator);
      childVectors.add(childVec);
    }

    // Assemble the StructVector
    StructVector structVec = (StructVector) field.createVector(allocator);
    structVec.setInitialCapacity(numRows);
    structVec.allocateNew();

    // Set struct validity to all-valid (PackedStruct does not support null rows in V2.0)
    for (int i = 0; i < numRows; i++) {
      org.apache.arrow.vector.BitVectorHelper.setValidityBit(
          structVec.getValidityBuffer(), i, 1);
    }

    // Transfer child data into struct slots
    for (int i = 0; i < children.size(); i++) {
      Field childField = children.get(i);
      FieldVector childVec = childVectors.get(i);
      @SuppressWarnings("unchecked")
      FieldVector slot = structVec.addOrGet(
          childField.getName(), childField.getFieldType(),
          (Class<? extends FieldVector>) childVec.getClass());
      childVec.makeTransferPair(slot).transfer();
      childVec.close();
    }

    structVec.setValueCount(numRows);
    return structVec;
  }

  /**
   * Returns the byte width of a fixed-width Arrow field.
   *
   * @throws UnsupportedOperationException if the field is not fixed-width
   */
  private static int getByteWidth(Field field) {
    ArrowType type = field.getType();
    if (type instanceof ArrowType.Int) {
      return ((ArrowType.Int) type).getBitWidth() / 8;
    }
    if (type instanceof ArrowType.FloatingPoint) {
      int precision = ((ArrowType.FloatingPoint) type).getPrecision().getFlatbufID();
      return switch (precision) {
        case 0 -> 2;  // HALF
        case 1 -> 4;  // SINGLE
        case 2 -> 8;  // DOUBLE
        default -> throw new UnsupportedOperationException(
            "Unsupported floating point precision: " + precision);
      };
    }
    if (type instanceof ArrowType.FixedSizeBinary) {
      return ((ArrowType.FixedSizeBinary) type).getByteWidth();
    }
    if (type instanceof ArrowType.Decimal) {
      return ((ArrowType.Decimal) type).getBitWidth() / 8;
    }
    if (type instanceof ArrowType.Bool) {
      return 1;
    }
    if (type instanceof ArrowType.Timestamp
        || type instanceof ArrowType.Date
        || type instanceof ArrowType.Time
        || type instanceof ArrowType.Duration
        || type instanceof ArrowType.Interval) {
      return 8;
    }
    if (type instanceof ArrowType.FixedSizeList) {
      int listSize = ((ArrowType.FixedSizeList) type).getListSize();
      Field elementField = field.getChildren().get(0);
      return listSize * getByteWidth(elementField);
    }
    throw new UnsupportedOperationException(
        "PackedStruct does not support variable-width or nested type: " + type);
  }

  /**
   * Builds a child vector from extracted packed bytes.
   *
   * <p>For most fixed-width types this delegates to {@link FixedWidthVectorBuilder}.
   * For {@link ArrowType.FixedSizeList} it manually assembles the vector because the
   * packed bytes are the flattened inner values.
   */
  private static FieldVector buildChildVector(
      Field childField, int numRows, byte[] childData,
      int childBits, BufferAllocator allocator) {
    ArrowType type = childField.getType();
    if (type instanceof ArrowType.FixedSizeList) {
      int dimension = ((ArrowType.FixedSizeList) type).getListSize();
      Field innerField = childField.getChildren().get(0);
      int innerBits = childBits / dimension;

      org.apache.arrow.vector.complex.FixedSizeListVector listVec =
          (org.apache.arrow.vector.complex.FixedSizeListVector)
              childField.createVector(allocator);
      listVec.setInitialCapacity(numRows);
      listVec.allocateNew();
      for (int i = 0; i < numRows; i++) {
        listVec.setNotNull(i);
      }

      ByteBuffer innerBuf = ByteBuffer.wrap(childData).order(ByteOrder.LITTLE_ENDIAN);
      FieldVector innerVec = FixedWidthVectorBuilder.build(
          innerField, numRows * dimension, innerBuf, innerBits, allocator);

      org.apache.arrow.vector.ValueVector slot =
          listVec.addOrGetVector(innerField.getFieldType()).getVector();
      innerVec.makeTransferPair(slot).transfer();
      innerVec.close();

      listVec.setValueCount(numRows);
      return listVec;
    }

    ByteBuffer childBuf = ByteBuffer.wrap(childData).order(ByteOrder.LITTLE_ENDIAN);
    return FixedWidthVectorBuilder.build(
        childField, numRows, childBuf, childBits, allocator);
  }
}
