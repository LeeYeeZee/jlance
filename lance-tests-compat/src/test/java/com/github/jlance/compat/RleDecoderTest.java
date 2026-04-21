package com.github.jlance.compat;

import static org.junit.jupiter.api.Assertions.*;

import com.github.jlance.format.buffer.PageBufferStore;
import com.github.jlance.format.decoder.RleDecoder;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import lance.encodings.EncodingsV20;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
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
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;

public class RleDecoderTest {

  private static EncodingsV20.ArrayEncoding makeRleEncoding(int bitsPerValue) {
    var rle = EncodingsV20.Rle.newBuilder().setBitsPerValue(bitsPerValue).build();
    return EncodingsV20.ArrayEncoding.newBuilder().setRle(rle).build();
  }

  /** Creates a dual-buffer RLE page store: buffer 0 = values, buffer 1 = lengths. */
  private static PageBufferStore makeDualBufferStore(byte[] values, byte[] lengths) {
    List<byte[]> buffers = new ArrayList<>();
    buffers.add(values);
    buffers.add(lengths);
    return new PageBufferStore(buffers);
  }

  /** Creates a block-format RLE page store: [8-byte LE values size][values][lengths]. */
  private static PageBufferStore makeBlockStore(byte[] values, byte[] lengths) {
    byte[] block = new byte[8 + values.length + lengths.length];
    ByteBuffer.wrap(block).order(ByteOrder.LITTLE_ENDIAN).putLong(values.length);
    System.arraycopy(values, 0, block, 8, values.length);
    System.arraycopy(lengths, 0, block, 8 + values.length, lengths.length);
    List<byte[]> buffers = new ArrayList<>();
    buffers.add(block);
    return new PageBufferStore(buffers);
  }

  @Test
  public void testBasicDualBufferInt32() throws Exception {
    // Input: [1, 1, 1, 2, 2, 3, 3, 3, 3]
    byte[] values = new byte[12]; // 3 i32 values
    ByteBuffer.wrap(values).order(ByteOrder.LITTLE_ENDIAN)
        .putInt(0, 1).putInt(4, 2).putInt(8, 3);
    byte[] lengths = {3, 2, 4};
    PageBufferStore store = makeDualBufferStore(values, lengths);
    EncodingsV20.ArrayEncoding encoding = makeRleEncoding(32);

    try (BufferAllocator allocator = new RootAllocator()) {
      Field field = new Field("a", FieldType.nullable(new ArrowType.Int(32, true)), null);
      RleDecoder decoder = new RleDecoder();
      FieldVector vector = decoder.decode(encoding, 9, store, field, allocator);
      try {
        IntVector intVec = (IntVector) vector;
        assertEquals(9, intVec.getValueCount());
        assertEquals(1, intVec.get(0));
        assertEquals(1, intVec.get(1));
        assertEquals(1, intVec.get(2));
        assertEquals(2, intVec.get(3));
        assertEquals(2, intVec.get(4));
        assertEquals(3, intVec.get(5));
        assertEquals(3, intVec.get(6));
        assertEquals(3, intVec.get(7));
        assertEquals(3, intVec.get(8));
      } finally {
        vector.close();
      }
    }
  }

  @Test
  public void testBasicBlockFormatInt32() throws Exception {
    byte[] values = new byte[12];
    ByteBuffer.wrap(values).order(ByteOrder.LITTLE_ENDIAN)
        .putInt(0, 1).putInt(4, 2).putInt(8, 3);
    byte[] lengths = {3, 2, 4};
    PageBufferStore store = makeBlockStore(values, lengths);
    EncodingsV20.ArrayEncoding encoding = makeRleEncoding(32);

    try (BufferAllocator allocator = new RootAllocator()) {
      Field field = new Field("a", FieldType.nullable(new ArrowType.Int(32, true)), null);
      RleDecoder decoder = new RleDecoder();
      FieldVector vector = decoder.decode(encoding, 9, store, field, allocator);
      try {
        IntVector intVec = (IntVector) vector;
        assertEquals(9, intVec.getValueCount());
        int[] expected = {1, 1, 1, 2, 2, 3, 3, 3, 3};
        for (int i = 0; i < expected.length; i++) {
          assertEquals(expected[i], intVec.get(i), "Mismatch at index " + i);
        }
      } finally {
        vector.close();
      }
    }
  }

  @Test
  public void testLongRunSplitting() throws Exception {
    // 1000 identical values of 42, then 300 identical values of 100
    // Runs: 255, 255, 255, 235 for 42; 255, 45 for 100
    byte[] values = new byte[6 * 4]; // 6 i32 values
    ByteBuffer.wrap(values).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < 4; i++) {
      ByteBuffer.wrap(values).order(ByteOrder.LITTLE_ENDIAN).putInt(i * 4, 42);
    }
    ByteBuffer.wrap(values).order(ByteOrder.LITTLE_ENDIAN).putInt(4 * 4, 100);
    ByteBuffer.wrap(values).order(ByteOrder.LITTLE_ENDIAN).putInt(5 * 4, 100);

    byte[] lengths = {(byte) 255, (byte) 255, (byte) 255, (byte) 235, (byte) 255, 45};
    PageBufferStore store = makeDualBufferStore(values, lengths);
    EncodingsV20.ArrayEncoding encoding = makeRleEncoding(32);

    try (BufferAllocator allocator = new RootAllocator()) {
      Field field = new Field("a", FieldType.nullable(new ArrowType.Int(32, true)), null);
      RleDecoder decoder = new RleDecoder();
      FieldVector vector = decoder.decode(encoding, 1300, store, field, allocator);
      try {
        IntVector intVec = (IntVector) vector;
        assertEquals(1300, intVec.getValueCount());
        for (int i = 0; i < 1000; i++) {
          assertEquals(42, intVec.get(i), "Mismatch at index " + i);
        }
        for (int i = 1000; i < 1300; i++) {
          assertEquals(100, intVec.get(i), "Mismatch at index " + i);
        }
      } finally {
        vector.close();
      }
    }
  }

  @Test
  public void testInt8() throws Exception {
    byte[] values = {42, 100, (byte) 255};
    byte[] lengths = {3, 2, 4};
    PageBufferStore store = makeDualBufferStore(values, lengths);
    EncodingsV20.ArrayEncoding encoding = makeRleEncoding(8);

    try (BufferAllocator allocator = new RootAllocator()) {
      Field field = new Field("a", FieldType.nullable(new ArrowType.Int(8, true)), null);
      RleDecoder decoder = new RleDecoder();
      FieldVector vector = decoder.decode(encoding, 9, store, field, allocator);
      try {
        TinyIntVector vec = (TinyIntVector) vector;
        assertEquals(9, vec.getValueCount());
        byte[] expected = {42, 42, 42, 100, 100, (byte) 255, (byte) 255, (byte) 255, (byte) 255};
        for (int i = 0; i < expected.length; i++) {
          assertEquals(expected[i], vec.get(i), "Mismatch at index " + i);
        }
      } finally {
        vector.close();
      }
    }
  }

  @Test
  public void testInt16() throws Exception {
    byte[] values = new byte[6];
    ByteBuffer.wrap(values).order(ByteOrder.LITTLE_ENDIAN)
        .putShort(0, (short) 1000).putShort(2, (short) 2000).putShort(4, (short) 3000);
    byte[] lengths = {2, 3, 1};
    PageBufferStore store = makeDualBufferStore(values, lengths);
    EncodingsV20.ArrayEncoding encoding = makeRleEncoding(16);

    try (BufferAllocator allocator = new RootAllocator()) {
      Field field = new Field("a", FieldType.nullable(new ArrowType.Int(16, true)), null);
      RleDecoder decoder = new RleDecoder();
      FieldVector vector = decoder.decode(encoding, 6, store, field, allocator);
      try {
        SmallIntVector vec = (SmallIntVector) vector;
        assertEquals(6, vec.getValueCount());
        short[] expected = {1000, 1000, 2000, 2000, 2000, 3000};
        for (int i = 0; i < expected.length; i++) {
          assertEquals(expected[i], vec.get(i), "Mismatch at index " + i);
        }
      } finally {
        vector.close();
      }
    }
  }

  @Test
  public void testInt64() throws Exception {
    byte[] values = new byte[16];
    ByteBuffer.wrap(values).order(ByteOrder.LITTLE_ENDIAN)
        .putLong(0, 1_000_000_000L).putLong(8, 2_000_000_000L);
    byte[] lengths = {3, 2};
    PageBufferStore store = makeDualBufferStore(values, lengths);
    EncodingsV20.ArrayEncoding encoding = makeRleEncoding(64);

    try (BufferAllocator allocator = new RootAllocator()) {
      Field field = new Field("a", FieldType.nullable(new ArrowType.Int(64, true)), null);
      RleDecoder decoder = new RleDecoder();
      FieldVector vector = decoder.decode(encoding, 5, store, field, allocator);
      try {
        BigIntVector vec = (BigIntVector) vector;
        assertEquals(5, vec.getValueCount());
        long[] expected = {1_000_000_000L, 1_000_000_000L, 1_000_000_000L, 2_000_000_000L, 2_000_000_000L};
        for (int i = 0; i < expected.length; i++) {
          assertEquals(expected[i], vec.get(i), "Mismatch at index " + i);
        }
      } finally {
        vector.close();
      }
    }
  }

  @Test
  public void testFloat32() throws Exception {
    byte[] values = new byte[8];
    ByteBuffer.wrap(values).order(ByteOrder.LITTLE_ENDIAN)
        .putFloat(0, 3.14f).putFloat(4, 2.71f);
    byte[] lengths = {2, 3};
    PageBufferStore store = makeDualBufferStore(values, lengths);
    EncodingsV20.ArrayEncoding encoding = makeRleEncoding(32);

    try (BufferAllocator allocator = new RootAllocator()) {
      Field field = new Field("a", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null);
      RleDecoder decoder = new RleDecoder();
      FieldVector vector = decoder.decode(encoding, 5, store, field, allocator);
      try {
        Float4Vector vec = (Float4Vector) vector;
        assertEquals(5, vec.getValueCount());
        assertEquals(3.14f, vec.get(0), 0.0001f);
        assertEquals(3.14f, vec.get(1), 0.0001f);
        assertEquals(2.71f, vec.get(2), 0.0001f);
        assertEquals(2.71f, vec.get(3), 0.0001f);
        assertEquals(2.71f, vec.get(4), 0.0001f);
      } finally {
        vector.close();
      }
    }
  }

  @Test
  public void testFloat64() throws Exception {
    byte[] values = new byte[16];
    ByteBuffer.wrap(values).order(ByteOrder.LITTLE_ENDIAN)
        .putDouble(0, 3.14159).putDouble(8, 2.71828);
    byte[] lengths = {1, 2};
    PageBufferStore store = makeDualBufferStore(values, lengths);
    EncodingsV20.ArrayEncoding encoding = makeRleEncoding(64);

    try (BufferAllocator allocator = new RootAllocator()) {
      Field field = new Field("a", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null);
      RleDecoder decoder = new RleDecoder();
      FieldVector vector = decoder.decode(encoding, 3, store, field, allocator);
      try {
        Float8Vector vec = (Float8Vector) vector;
        assertEquals(3, vec.getValueCount());
        assertEquals(3.14159, vec.get(0), 0.00001);
        assertEquals(2.71828, vec.get(1), 0.00001);
        assertEquals(2.71828, vec.get(2), 0.00001);
      } finally {
        vector.close();
      }
    }
  }

  @Test
  public void testUInt1() throws Exception {
    byte[] values = {1, 0};
    byte[] lengths = {3, 2};
    PageBufferStore store = makeDualBufferStore(values, lengths);
    EncodingsV20.ArrayEncoding encoding = makeRleEncoding(8);

    try (BufferAllocator allocator = new RootAllocator()) {
      Field field = new Field("a", FieldType.nullable(new ArrowType.Int(8, false)), null);
      RleDecoder decoder = new RleDecoder();
      FieldVector vector = decoder.decode(encoding, 5, store, field, allocator);
      try {
        UInt1Vector vec = (UInt1Vector) vector;
        assertEquals(5, vec.getValueCount());
        assertEquals(1, vec.get(0));
        assertEquals(1, vec.get(1));
        assertEquals(1, vec.get(2));
        assertEquals(0, vec.get(3));
        assertEquals(0, vec.get(4));
      } finally {
        vector.close();
      }
    }
  }

  @Test
  public void testUInt2() throws Exception {
    byte[] values = new byte[4];
    ByteBuffer.wrap(values).order(ByteOrder.LITTLE_ENDIAN)
        .putShort(0, (short) 100).putShort(2, (short) 200);
    byte[] lengths = {2, 3};
    PageBufferStore store = makeDualBufferStore(values, lengths);
    EncodingsV20.ArrayEncoding encoding = makeRleEncoding(16);

    try (BufferAllocator allocator = new RootAllocator()) {
      Field field = new Field("a", FieldType.nullable(new ArrowType.Int(16, false)), null);
      RleDecoder decoder = new RleDecoder();
      FieldVector vector = decoder.decode(encoding, 5, store, field, allocator);
      try {
        UInt2Vector vec = (UInt2Vector) vector;
        assertEquals(5, vec.getValueCount());
        assertEquals(100, vec.get(0));
        assertEquals(100, vec.get(1));
        assertEquals(200, vec.get(2));
        assertEquals(200, vec.get(3));
        assertEquals(200, vec.get(4));
      } finally {
        vector.close();
      }
    }
  }

  @Test
  public void testUInt4() throws Exception {
    byte[] values = new byte[8];
    ByteBuffer.wrap(values).order(ByteOrder.LITTLE_ENDIAN)
        .putInt(0, 1_000).putInt(4, 2_000);
    byte[] lengths = {2, 3};
    PageBufferStore store = makeDualBufferStore(values, lengths);
    EncodingsV20.ArrayEncoding encoding = makeRleEncoding(32);

    try (BufferAllocator allocator = new RootAllocator()) {
      Field field = new Field("a", FieldType.nullable(new ArrowType.Int(32, false)), null);
      RleDecoder decoder = new RleDecoder();
      FieldVector vector = decoder.decode(encoding, 5, store, field, allocator);
      try {
        UInt4Vector vec = (UInt4Vector) vector;
        assertEquals(5, vec.getValueCount());
        assertEquals(1_000, vec.get(0));
        assertEquals(1_000, vec.get(1));
        assertEquals(2_000, vec.get(2));
        assertEquals(2_000, vec.get(3));
        assertEquals(2_000, vec.get(4));
      } finally {
        vector.close();
      }
    }
  }

  @Test
  public void testUInt8() throws Exception {
    byte[] values = new byte[16];
    ByteBuffer.wrap(values).order(ByteOrder.LITTLE_ENDIAN)
        .putLong(0, 1_000_000_000L).putLong(8, 2_000_000_000L);
    byte[] lengths = {1, 1};
    PageBufferStore store = makeDualBufferStore(values, lengths);
    EncodingsV20.ArrayEncoding encoding = makeRleEncoding(64);

    try (BufferAllocator allocator = new RootAllocator()) {
      Field field = new Field("a", FieldType.nullable(new ArrowType.Int(64, false)), null);
      RleDecoder decoder = new RleDecoder();
      FieldVector vector = decoder.decode(encoding, 2, store, field, allocator);
      try {
        UInt8Vector vec = (UInt8Vector) vector;
        assertEquals(2, vec.getValueCount());
        assertEquals(1_000_000_000L, vec.get(0));
        assertEquals(2_000_000_000L, vec.get(1));
      } finally {
        vector.close();
      }
    }
  }

  @Test
  public void testEmptyData() throws Exception {
    PageBufferStore store = makeDualBufferStore(new byte[0], new byte[0]);
    EncodingsV20.ArrayEncoding encoding = makeRleEncoding(32);

    try (BufferAllocator allocator = new RootAllocator()) {
      Field field = new Field("a", FieldType.nullable(new ArrowType.Int(32, true)), null);
      RleDecoder decoder = new RleDecoder();
      FieldVector vector = decoder.decode(encoding, 0, store, field, allocator);
      try {
        IntVector intVec = (IntVector) vector;
        assertEquals(0, intVec.getValueCount());
      } finally {
        vector.close();
      }
    }
  }

  @Test
  public void testNoBuffersZeroRows() throws Exception {
    List<byte[]> buffers = new ArrayList<>();
    PageBufferStore store = new PageBufferStore(buffers);
    EncodingsV20.ArrayEncoding encoding = makeRleEncoding(32);

    try (BufferAllocator allocator = new RootAllocator()) {
      Field field = new Field("a", FieldType.nullable(new ArrowType.Int(32, true)), null);
      RleDecoder decoder = new RleDecoder();
      FieldVector vector = decoder.decode(encoding, 0, store, field, allocator);
      try {
        IntVector intVec = (IntVector) vector;
        assertEquals(0, intVec.getValueCount());
      } finally {
        vector.close();
      }
    }
  }

  @Test
  public void testNumRowsLessThanTotalRuns() throws Exception {
    // Input: [1, 1, 1, 2, 2, 3, 3, 3, 3] but only request 4 rows
    byte[] values = new byte[12];
    ByteBuffer.wrap(values).order(ByteOrder.LITTLE_ENDIAN)
        .putInt(0, 1).putInt(4, 2).putInt(8, 3);
    byte[] lengths = {3, 2, 4};
    PageBufferStore store = makeDualBufferStore(values, lengths);
    EncodingsV20.ArrayEncoding encoding = makeRleEncoding(32);

    try (BufferAllocator allocator = new RootAllocator()) {
      Field field = new Field("a", FieldType.nullable(new ArrowType.Int(32, true)), null);
      RleDecoder decoder = new RleDecoder();
      FieldVector vector = decoder.decode(encoding, 4, store, field, allocator);
      try {
        IntVector intVec = (IntVector) vector;
        assertEquals(4, intVec.getValueCount());
        assertEquals(1, intVec.get(0));
        assertEquals(1, intVec.get(1));
        assertEquals(1, intVec.get(2));
        assertEquals(2, intVec.get(3));
      } finally {
        vector.close();
      }
    }
  }

  @Test
  public void testInconsistentBuffersThrows() throws Exception {
    byte[] values = new byte[12]; // 3 i32 values
    byte[] lengths = {3, 2}; // only 2 length entries
    PageBufferStore store = makeDualBufferStore(values, lengths);
    EncodingsV20.ArrayEncoding encoding = makeRleEncoding(32);

    try (BufferAllocator allocator = new RootAllocator()) {
      Field field = new Field("a", FieldType.nullable(new ArrowType.Int(32, true)), null);
      RleDecoder decoder = new RleDecoder();
      IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> {
        decoder.decode(encoding, 9, store, field, allocator);
      });
      assertTrue(ex.getMessage().contains("inconsistent buffers"));
    }
  }

  @Test
  public void testZeroRunLengthThrows() throws Exception {
    byte[] values = new byte[4];
    ByteBuffer.wrap(values).order(ByteOrder.LITTLE_ENDIAN).putInt(0, 42);
    byte[] lengths = {0};
    PageBufferStore store = makeDualBufferStore(values, lengths);
    EncodingsV20.ArrayEncoding encoding = makeRleEncoding(32);

    try (BufferAllocator allocator = new RootAllocator()) {
      Field field = new Field("a", FieldType.nullable(new ArrowType.Int(32, true)), null);
      RleDecoder decoder = new RleDecoder();
      IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> {
        decoder.decode(encoding, 1, store, field, allocator);
      });
      assertTrue(ex.getMessage().contains("zero run length"));
    }
  }

  @Test
  public void testBlockBufferTooSmallThrows() throws Exception {
    byte[] block = {1, 2, 3}; // less than 8 bytes
    List<byte[]> buffers = new ArrayList<>();
    buffers.add(block);
    PageBufferStore store = new PageBufferStore(buffers);
    EncodingsV20.ArrayEncoding encoding = makeRleEncoding(32);

    try (BufferAllocator allocator = new RootAllocator()) {
      Field field = new Field("a", FieldType.nullable(new ArrowType.Int(32, true)), null);
      RleDecoder decoder = new RleDecoder();
      IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> {
        decoder.decode(encoding, 1, store, field, allocator);
      });
      assertTrue(ex.getMessage().contains("too small"));
    }
  }

  @Test
  public void testUnsupportedBitsPerValueThrows() throws Exception {
    EncodingsV20.ArrayEncoding encoding = makeRleEncoding(128);
    try (BufferAllocator allocator = new RootAllocator()) {
      Field field = new Field("a", FieldType.nullable(new ArrowType.Int(32, true)), null);
      RleDecoder decoder = new RleDecoder();
      IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> {
        decoder.decode(encoding, 1, new PageBufferStore(new ArrayList<>()), field, allocator);
      });
      assertTrue(ex.getMessage().contains("only supports 8, 16, 32, or 64 bits"));
    }
  }
}
