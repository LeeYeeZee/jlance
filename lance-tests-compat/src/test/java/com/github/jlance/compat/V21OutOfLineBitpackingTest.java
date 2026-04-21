package com.github.jlance.compat;

import static org.junit.jupiter.api.Assertions.*;

import com.github.jlance.format.buffer.PageBufferStore;
import com.github.jlance.format.decoder.CompressiveEncodingDecoders;
import com.github.jlance.format.decoder.FixedWidthVectorBuilder;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import lance.encodings21.EncodingsV21;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for V2.1+ OutOfLineBitpacking decoding.
 */
public class V21OutOfLineBitpackingTest {

  private static Field int32Field(String name) {
    return new Field(name, FieldType.nullable(new ArrowType.Int(32, true)), null);
  }

  @Test
  public void testOutOfLineBitpackingU32RawTail() throws Exception {
    try (BufferAllocator allocator = new RootAllocator()) {
      int numValues = 5;
      int[] values = {10, 20, 30, 40, 50};

      // bitWidth=32: no actual packing, tail is raw
      byte[] packed = new byte[numValues * 4];
      ByteBuffer.wrap(packed).order(ByteOrder.LITTLE_ENDIAN)
          .putInt(10).putInt(20).putInt(30).putInt(40).putInt(50);

      var ool = EncodingsV21.OutOfLineBitpacking.newBuilder()
          .setUncompressedBitsPerValue(32)
          .setValues(EncodingsV21.CompressiveEncoding.newBuilder()
              .setFlat(EncodingsV21.Flat.newBuilder().setBitsPerValue(32).build())
              .build())
          .build();
      var compressive = EncodingsV21.CompressiveEncoding.newBuilder()
          .setOutOfLineBitpacking(ool)
          .build();

      PageBufferStore store = new PageBufferStore(List.of(packed));
      ByteBuffer buf = CompressiveEncodingDecoders.decode(compressive, numValues, store, allocator);
      FieldVector vector = FixedWidthVectorBuilder.build(
          int32Field("x"), numValues, buf, 32, allocator);

      assertInstanceOf(IntVector.class, vector);
      IntVector intVec = (IntVector) vector;
      assertEquals(numValues, intVec.getValueCount());
      assertEquals(10, intVec.get(0));
      assertEquals(20, intVec.get(1));
      assertEquals(30, intVec.get(2));
      assertEquals(40, intVec.get(3));
      assertEquals(50, intVec.get(4));
      vector.close();
    }
  }

  @Test
  public void testOutOfLineBitpackingU32WholeChunkPlusRawTail() throws Exception {
    try (BufferAllocator allocator = new RootAllocator()) {
      int numValues = 1025;
      int[] values = new int[numValues];
      for (int i = 0; i < numValues; i++) {
        values[i] = i * 3;
      }

      // bitWidth=32: one whole chunk (1024 ints) + raw tail (1 int)
      byte[] packed = new byte[numValues * 4];
      ByteBuffer bb = ByteBuffer.wrap(packed).order(ByteOrder.LITTLE_ENDIAN);
      for (int i = 0; i < numValues; i++) {
        bb.putInt(values[i]);
      }

      var ool = EncodingsV21.OutOfLineBitpacking.newBuilder()
          .setUncompressedBitsPerValue(32)
          .setValues(EncodingsV21.CompressiveEncoding.newBuilder()
              .setFlat(EncodingsV21.Flat.newBuilder().setBitsPerValue(32).build())
              .build())
          .build();
      var compressive = EncodingsV21.CompressiveEncoding.newBuilder()
          .setOutOfLineBitpacking(ool)
          .build();

      PageBufferStore store = new PageBufferStore(List.of(packed));
      ByteBuffer buf = CompressiveEncodingDecoders.decode(compressive, numValues, store, allocator);
      FieldVector vector = FixedWidthVectorBuilder.build(
          int32Field("x"), numValues, buf, 32, allocator);

      assertInstanceOf(IntVector.class, vector);
      IntVector intVec = (IntVector) vector;
      assertEquals(numValues, intVec.getValueCount());
      for (int i = 0; i < numValues; i++) {
        assertEquals(values[i], intVec.get(i), "Mismatch at index " + i);
      }
      vector.close();
    }
  }

  @Test
  public void testOutOfLineBitpackingU32Packed4Bits() throws Exception {
    try (BufferAllocator allocator = new RootAllocator()) {
      int numValues = 1024;
      int[] values = new int[numValues];
      for (int i = 0; i < numValues; i++) {
        values[i] = i % 16; // fit in 4 bits
      }

      // Pack using FastLanes
      int compressedBits = 4;
      int wordsPerChunk = (1024 * compressedBits) / 32; // 128
      int[] packedInts = new int[wordsPerChunk];
      java.util.Arrays.fill(packedInts, 0);
      com.github.jlance.format.decoder.FastLanesBitPacking.packU32(
          compressedBits, values, 0, packedInts, 0);

      byte[] packed = new byte[packedInts.length * 4];
      ByteBuffer.wrap(packed).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer().put(packedInts);

      var ool = EncodingsV21.OutOfLineBitpacking.newBuilder()
          .setUncompressedBitsPerValue(32)
          .setValues(EncodingsV21.CompressiveEncoding.newBuilder()
              .setFlat(EncodingsV21.Flat.newBuilder().setBitsPerValue(compressedBits).build())
              .build())
          .build();
      var compressive = EncodingsV21.CompressiveEncoding.newBuilder()
          .setOutOfLineBitpacking(ool)
          .build();

      PageBufferStore store = new PageBufferStore(List.of(packed));
      ByteBuffer buf = CompressiveEncodingDecoders.decode(compressive, numValues, store, allocator);
      FieldVector vector = FixedWidthVectorBuilder.build(
          int32Field("x"), numValues, buf, 32, allocator);

      assertInstanceOf(IntVector.class, vector);
      IntVector intVec = (IntVector) vector;
      assertEquals(numValues, intVec.getValueCount());
      for (int i = 0; i < numValues; i++) {
        assertEquals(values[i], intVec.get(i), "Mismatch at index " + i);
      }
      vector.close();
    }
  }

  @Test
  public void testOutOfLineBitpackingU32Packed4BitsWithTail() throws Exception {
    try (BufferAllocator allocator = new RootAllocator()) {
      int numValues = 1025;
      int[] values = new int[numValues];
      for (int i = 0; i < numValues; i++) {
        values[i] = i % 16;
      }

      int compressedBits = 4;
      int wordsPerChunk = (1024 * compressedBits) / 32; // 128
      int tailValues = numValues % 1024; // 1

      // For bitWidth=4, tail_bit_savings=28, padding_cost=4*(1024-1)=4092
      // tail_pack_savings = 28*1 = 28 < 4092, so tail is raw
      int[] packedInts = new int[wordsPerChunk + tailValues];
      java.util.Arrays.fill(packedInts, 0);
      com.github.jlance.format.decoder.FastLanesBitPacking.packU32(
          compressedBits, values, 0, packedInts, 0);
      // Append raw tail value
      packedInts[wordsPerChunk] = values[1024];

      byte[] packed = new byte[packedInts.length * 4];
      ByteBuffer.wrap(packed).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer().put(packedInts);

      var ool = EncodingsV21.OutOfLineBitpacking.newBuilder()
          .setUncompressedBitsPerValue(32)
          .setValues(EncodingsV21.CompressiveEncoding.newBuilder()
              .setFlat(EncodingsV21.Flat.newBuilder().setBitsPerValue(compressedBits).build())
              .build())
          .build();
      var compressive = EncodingsV21.CompressiveEncoding.newBuilder()
          .setOutOfLineBitpacking(ool)
          .build();

      PageBufferStore store = new PageBufferStore(List.of(packed));
      ByteBuffer buf = CompressiveEncodingDecoders.decode(compressive, numValues, store, allocator);
      FieldVector vector = FixedWidthVectorBuilder.build(
          int32Field("x"), numValues, buf, 32, allocator);

      assertInstanceOf(IntVector.class, vector);
      IntVector intVec = (IntVector) vector;
      assertEquals(numValues, intVec.getValueCount());
      for (int i = 0; i < numValues; i++) {
        assertEquals(values[i], intVec.get(i), "Mismatch at index " + i);
      }
      vector.close();
    }
  }
}
