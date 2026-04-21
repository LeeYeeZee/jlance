package com.github.jlance.format.decoder;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;

/**
 * Shared helper to set a constant value into an Arrow vector at a given index.
 *
 * <p>Used by both V2.0 {@link ConstantDecoder} and V2.1+ {@link ConstantLayoutDecoder}.
 */
public final class ConstantValueSetter {

  private ConstantValueSetter() {}

  /**
   * Sets a single constant value into the vector at the given index.
   *
   * @param vector the target vector
   * @param valueBytes the little-endian raw bytes of the constant value
   * @param index the row index to set
   */
  public static void setValue(FieldVector vector, byte[] valueBytes, int index) {
    if (vector instanceof IntVector intVec) {
      int value = ByteBuffer.wrap(valueBytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
      intVec.set(index, value);
    } else if (vector instanceof BigIntVector bigIntVec) {
      long value = ByteBuffer.wrap(valueBytes).order(ByteOrder.LITTLE_ENDIAN).getLong();
      bigIntVec.set(index, value);
    } else if (vector instanceof Float4Vector float4Vec) {
      float value = ByteBuffer.wrap(valueBytes).order(ByteOrder.LITTLE_ENDIAN).getFloat();
      float4Vec.set(index, value);
    } else if (vector instanceof Float8Vector float8Vec) {
      double value = ByteBuffer.wrap(valueBytes).order(ByteOrder.LITTLE_ENDIAN).getDouble();
      float8Vec.set(index, value);
    } else if (vector instanceof VarCharVector varCharVec) {
      varCharVec.setSafe(index, valueBytes);
    } else if (vector instanceof VarBinaryVector varBinaryVec) {
      varBinaryVec.setSafe(index, valueBytes);
    } else if (vector instanceof BitVector bitVec) {
      boolean value = valueBytes.length > 0 && valueBytes[0] != 0;
      bitVec.set(index, value ? 1 : 0);
    } else if (vector instanceof TinyIntVector tinyIntVec) {
      byte value = valueBytes.length > 0 ? valueBytes[0] : 0;
      tinyIntVec.set(index, value);
    } else if (vector instanceof SmallIntVector smallIntVec) {
      short value = ByteBuffer.wrap(valueBytes).order(ByteOrder.LITTLE_ENDIAN).getShort();
      smallIntVec.set(index, value);
    } else if (vector instanceof UInt1Vector uInt1Vec) {
      byte value = valueBytes.length > 0 ? valueBytes[0] : 0;
      uInt1Vec.set(index, value);
    } else if (vector instanceof UInt2Vector uInt2Vec) {
      char value = (char) ByteBuffer.wrap(valueBytes).order(ByteOrder.LITTLE_ENDIAN).getShort();
      uInt2Vec.set(index, value);
    } else if (vector instanceof UInt4Vector uInt4Vec) {
      int value = ByteBuffer.wrap(valueBytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
      uInt4Vec.set(index, value);
    } else if (vector instanceof UInt8Vector uInt8Vec) {
      long value = ByteBuffer.wrap(valueBytes).order(ByteOrder.LITTLE_ENDIAN).getLong();
      uInt8Vec.set(index, value);
    } else {
      throw new UnsupportedOperationException(
          "Constant value not supported for vector type: " + vector.getClass().getName());
    }
  }

  /** Copies a value from one index to another within the same vector. */
  public static void copyValue(FieldVector vector, int fromIndex, int toIndex) {
    vector.copyFromSafe(fromIndex, toIndex, vector);
  }
}
