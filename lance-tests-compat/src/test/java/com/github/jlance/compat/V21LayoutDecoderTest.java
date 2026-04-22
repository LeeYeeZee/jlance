// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.compat;

import static org.junit.jupiter.api.Assertions.*;

import com.github.jlance.format.buffer.PageBufferStore;
import com.github.jlance.format.decoder.ConstantLayoutDecoder;
import com.github.jlance.format.decoder.FullZipLayoutDecoder;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import lance.encodings21.EncodingsV21;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for V2.1+ {@link ConstantLayoutDecoder} and {@link FullZipLayoutDecoder}.
 *
 * <p>These tests construct synthetic {@link EncodingsV21.PageLayout} protobuf messages
 * and page buffers directly, since Python Lance does not yet provide a way to control
 * the output file version.
 */
public class V21LayoutDecoderTest {

  private static Field int32Field(String name) {
    return new Field(name, FieldType.nullable(new ArrowType.Int(32, true)), null);
  }

  private static Field float64Field(String name) {
    return new Field(name, FieldType.nullable(new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)), null);
  }

  private static Field int64Field(String name) {
    return new Field(name, FieldType.nullable(new ArrowType.Int(64, true)), null);
  }

  // ==================== ConstantLayout tests ====================

  @Test
  public void testConstantLayoutAllSameInt32() {
    try (BufferAllocator allocator = new RootAllocator()) {
      int numRows = 5;
      int value = 42;
      byte[] valueBytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array();

      var constantLayout = EncodingsV21.ConstantLayout.newBuilder()
          .setInlineValue(com.google.protobuf.ByteString.copyFrom(valueBytes))
          .build();
      var pageLayout = EncodingsV21.PageLayout.newBuilder()
          .setConstantLayout(constantLayout)
          .build();

      PageBufferStore store = new PageBufferStore(List.of());
      ConstantLayoutDecoder decoder = new ConstantLayoutDecoder();
      FieldVector vector = decoder.decode(pageLayout, numRows, store, int32Field("x"), allocator);

      assertInstanceOf(IntVector.class, vector);
      IntVector intVec = (IntVector) vector;
      assertEquals(numRows, intVec.getValueCount());
      for (int i = 0; i < numRows; i++) {
        assertEquals(value, intVec.get(i));
      }
      vector.close();
    }
  }

  @Test
  public void testConstantLayoutAllNullInt32() {
    try (BufferAllocator allocator = new RootAllocator()) {
      int numRows = 5;

      var constantLayout = EncodingsV21.ConstantLayout.newBuilder().build();
      var pageLayout = EncodingsV21.PageLayout.newBuilder()
          .setConstantLayout(constantLayout)
          .build();

      PageBufferStore store = new PageBufferStore(List.of());
      ConstantLayoutDecoder decoder = new ConstantLayoutDecoder();
      FieldVector vector = decoder.decode(pageLayout, numRows, store, int32Field("x"), allocator);

      assertInstanceOf(IntVector.class, vector);
      IntVector intVec = (IntVector) vector;
      assertEquals(numRows, intVec.getValueCount());
      for (int i = 0; i < numRows; i++) {
        assertTrue(intVec.isNull(i));
      }
      vector.close();
    }
  }

  @Test
  public void testConstantLayoutAllSameFloat64() {
    try (BufferAllocator allocator = new RootAllocator()) {
      int numRows = 3;
      double value = 3.14;
      byte[] valueBytes = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putDouble(value).array();

      var constantLayout = EncodingsV21.ConstantLayout.newBuilder()
          .setInlineValue(com.google.protobuf.ByteString.copyFrom(valueBytes))
          .build();
      var pageLayout = EncodingsV21.PageLayout.newBuilder()
          .setConstantLayout(constantLayout)
          .build();

      PageBufferStore store = new PageBufferStore(List.of());
      ConstantLayoutDecoder decoder = new ConstantLayoutDecoder();
      FieldVector vector = decoder.decode(pageLayout, numRows, store, float64Field("y"), allocator);

      assertInstanceOf(Float8Vector.class, vector);
      Float8Vector floatVec = (Float8Vector) vector;
      assertEquals(numRows, floatVec.getValueCount());
      for (int i = 0; i < numRows; i++) {
        assertEquals(value, floatVec.get(i), 0.0001);
      }
      vector.close();
    }
  }

  @Test
  public void testConstantLayoutEmptyPage() {
    try (BufferAllocator allocator = new RootAllocator()) {
      int numRows = 0;
      byte[] valueBytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(99).array();

      var constantLayout = EncodingsV21.ConstantLayout.newBuilder()
          .setInlineValue(com.google.protobuf.ByteString.copyFrom(valueBytes))
          .build();
      var pageLayout = EncodingsV21.PageLayout.newBuilder()
          .setConstantLayout(constantLayout)
          .build();

      PageBufferStore store = new PageBufferStore(List.of());
      ConstantLayoutDecoder decoder = new ConstantLayoutDecoder();
      FieldVector vector = decoder.decode(pageLayout, numRows, store, int32Field("x"), allocator);

      assertEquals(0, vector.getValueCount());
      vector.close();
    }
  }

  @Test
  public void testConstantLayoutNullableInt32() {
    try (BufferAllocator allocator = new RootAllocator()) {
      int numRows = 5;
      int value = 42;
      byte[] valueBytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array();
      // Validity: rows 0,2,4 valid; rows 1,3 null → bitmap = 0b00010101 = 0x15
      byte[] defBitmap = new byte[] {0x15};

      var constantLayout = EncodingsV21.ConstantLayout.newBuilder()
          .addLayers(EncodingsV21.RepDefLayer.REPDEF_NULLABLE_ITEM)
          .setInlineValue(com.google.protobuf.ByteString.copyFrom(valueBytes))
          .setNumDefValues(numRows)
          .build();
      var pageLayout = EncodingsV21.PageLayout.newBuilder()
          .setConstantLayout(constantLayout)
          .build();

      PageBufferStore store = new PageBufferStore(List.of(defBitmap));
      ConstantLayoutDecoder decoder = new ConstantLayoutDecoder();
      FieldVector vector = decoder.decode(pageLayout, numRows, store, int32Field("x"), allocator);

      assertInstanceOf(IntVector.class, vector);
      IntVector intVec = (IntVector) vector;
      assertEquals(numRows, intVec.getValueCount());
      assertEquals(value, intVec.get(0));
      assertTrue(intVec.isNull(1));
      assertEquals(value, intVec.get(2));
      assertTrue(intVec.isNull(3));
      assertEquals(value, intVec.get(4));
      vector.close();
    }
  }

  @Test
  public void testConstantLayoutNullableAllValid() {
    try (BufferAllocator allocator = new RootAllocator()) {
      int numRows = 3;
      int value = 99;
      byte[] valueBytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array();
      // All valid: bitmap = 0b00000111 = 0x07
      byte[] defBitmap = new byte[] {0x07};

      var constantLayout = EncodingsV21.ConstantLayout.newBuilder()
          .addLayers(EncodingsV21.RepDefLayer.REPDEF_NULLABLE_ITEM)
          .setInlineValue(com.google.protobuf.ByteString.copyFrom(valueBytes))
          .setNumDefValues(numRows)
          .build();
      var pageLayout = EncodingsV21.PageLayout.newBuilder()
          .setConstantLayout(constantLayout)
          .build();

      PageBufferStore store = new PageBufferStore(List.of(defBitmap));
      ConstantLayoutDecoder decoder = new ConstantLayoutDecoder();
      FieldVector vector = decoder.decode(pageLayout, numRows, store, int32Field("x"), allocator);

      assertInstanceOf(IntVector.class, vector);
      IntVector intVec = (IntVector) vector;
      assertEquals(numRows, intVec.getValueCount());
      for (int i = 0; i < numRows; i++) {
        assertEquals(value, intVec.get(i));
      }
      vector.close();
    }
  }

  @Test
  public void testConstantLayoutWithListLayers() {
    try (BufferAllocator allocator = new RootAllocator()) {
      var constantLayout = EncodingsV21.ConstantLayout.newBuilder()
          .addLayers(EncodingsV21.RepDefLayer.REPDEF_NULLABLE_LIST)
          .build();
      var pageLayout = EncodingsV21.PageLayout.newBuilder()
          .setConstantLayout(constantLayout)
          .build();

      PageBufferStore store = new PageBufferStore(List.of());
      ConstantLayoutDecoder decoder = new ConstantLayoutDecoder();
      try (org.apache.arrow.vector.FieldVector vec =
          decoder.decode(pageLayout, 5, store, int32Field("x"), allocator)) {
        assertEquals(5, vec.getValueCount());
        for (int i = 0; i < 5; i++) {
          assertTrue(vec.isNull(i));
        }
      }
    }
  }

  // ==================== FullZipLayout tests ====================

  @Test
  public void testFullZipLayoutFlatInt32() {
    try (BufferAllocator allocator = new RootAllocator()) {
      int numRows = 4;
      int[] values = {10, 20, 30, 40};
      byte[] valueBuffer = new byte[numRows * 4];
      ByteBuffer.wrap(valueBuffer).order(ByteOrder.LITTLE_ENDIAN)
          .putInt(values[0]).putInt(values[1]).putInt(values[2]).putInt(values[3]);

      var flat = EncodingsV21.Flat.newBuilder().setBitsPerValue(32).build();
      var valueCompression = EncodingsV21.CompressiveEncoding.newBuilder().setFlat(flat).build();
      var fullZip = EncodingsV21.FullZipLayout.newBuilder()
          .setBitsRep(0)
          .setBitsDef(0)
          .setBitsPerValue(32)
          .setNumItems(numRows)
          .setNumVisibleItems(numRows)
          .setValueCompression(valueCompression)
          .build();
      var pageLayout = EncodingsV21.PageLayout.newBuilder()
          .setFullZipLayout(fullZip)
          .build();

      PageBufferStore store = new PageBufferStore(List.of(valueBuffer));
      FullZipLayoutDecoder decoder = new FullZipLayoutDecoder();
      FieldVector vector = decoder.decode(pageLayout, numRows, store, int32Field("x"), allocator);

      assertInstanceOf(IntVector.class, vector);
      IntVector intVec = (IntVector) vector;
      assertEquals(numRows, intVec.getValueCount());
      for (int i = 0; i < numRows; i++) {
        assertEquals(values[i], intVec.get(i));
      }
      vector.close();
    }
  }

  @Test
  public void testFullZipLayoutFlatInt64() {
    try (BufferAllocator allocator = new RootAllocator()) {
      int numRows = 3;
      long[] values = {100L, 200L, 300L};
      byte[] valueBuffer = new byte[numRows * 8];
      ByteBuffer buf = ByteBuffer.wrap(valueBuffer).order(ByteOrder.LITTLE_ENDIAN);
      for (long v : values) {
        buf.putLong(v);
      }

      var flat = EncodingsV21.Flat.newBuilder().setBitsPerValue(64).build();
      var valueCompression = EncodingsV21.CompressiveEncoding.newBuilder().setFlat(flat).build();
      var fullZip = EncodingsV21.FullZipLayout.newBuilder()
          .setBitsRep(0)
          .setBitsDef(0)
          .setBitsPerValue(64)
          .setNumItems(numRows)
          .setNumVisibleItems(numRows)
          .setValueCompression(valueCompression)
          .build();
      var pageLayout = EncodingsV21.PageLayout.newBuilder()
          .setFullZipLayout(fullZip)
          .build();

      PageBufferStore store = new PageBufferStore(List.of(valueBuffer));
      FullZipLayoutDecoder decoder = new FullZipLayoutDecoder();
      FieldVector vector = decoder.decode(pageLayout, numRows, store, int64Field("z"), allocator);

      assertInstanceOf(BigIntVector.class, vector);
      BigIntVector bigIntVec = (BigIntVector) vector;
      assertEquals(numRows, bigIntVec.getValueCount());
      for (int i = 0; i < numRows; i++) {
        assertEquals(values[i], bigIntVec.get(i));
      }
      vector.close();
    }
  }

  @Test
  public void testFullZipLayoutWithRepLevelsThrows() {
    try (BufferAllocator allocator = new RootAllocator()) {
      var flat = EncodingsV21.Flat.newBuilder().setBitsPerValue(32).build();
      var valueCompression = EncodingsV21.CompressiveEncoding.newBuilder().setFlat(flat).build();
      var fullZip = EncodingsV21.FullZipLayout.newBuilder()
          .setBitsRep(16)
          .setBitsDef(0)
          .setBitsPerValue(32)
          .setNumItems(4)
          .setNumVisibleItems(4)
          .setValueCompression(valueCompression)
          .build();
      var pageLayout = EncodingsV21.PageLayout.newBuilder()
          .setFullZipLayout(fullZip)
          .build();

      PageBufferStore store = new PageBufferStore(List.of(new byte[16]));
      FullZipLayoutDecoder decoder = new FullZipLayoutDecoder();
      assertThrows(UnsupportedOperationException.class, () ->
          decoder.decode(pageLayout, 4, store, int32Field("x"), allocator));
    }
  }

  @Test
  public void testFullZipLayoutNullableInt32() {
    try (BufferAllocator allocator = new RootAllocator()) {
      int numRows = 5;
      // Valid rows: 0,2,4 → values 10, 30, 50
      int[] visibleValues = {10, 30, 50};
      byte[] valueBuffer = new byte[visibleValues.length * 4];
      ByteBuffer.wrap(valueBuffer).order(ByteOrder.LITTLE_ENDIAN)
          .putInt(visibleValues[0]).putInt(visibleValues[1]).putInt(visibleValues[2]);
      // Validity: rows 0,2,4 valid; rows 1,3 null → bitmap = 0b00010101 = 0x15
      byte[] defBitmap = new byte[] {0x15};

      var flat = EncodingsV21.Flat.newBuilder().setBitsPerValue(32).build();
      var valueCompression = EncodingsV21.CompressiveEncoding.newBuilder().setFlat(flat).build();
      var fullZip = EncodingsV21.FullZipLayout.newBuilder()
          .setBitsRep(0)
          .setBitsDef(1)
          .setBitsPerValue(32)
          .setNumItems(numRows)
          .setNumVisibleItems(visibleValues.length)
          .addLayers(EncodingsV21.RepDefLayer.REPDEF_NULLABLE_ITEM)
          .setValueCompression(valueCompression)
          .build();
      var pageLayout = EncodingsV21.PageLayout.newBuilder()
          .setFullZipLayout(fullZip)
          .build();

      PageBufferStore store = new PageBufferStore(List.of(defBitmap, valueBuffer));
      FullZipLayoutDecoder decoder = new FullZipLayoutDecoder();
      FieldVector vector = decoder.decode(pageLayout, numRows, store, int32Field("x"), allocator);

      assertInstanceOf(IntVector.class, vector);
      IntVector intVec = (IntVector) vector;
      assertEquals(numRows, intVec.getValueCount());
      assertEquals(10, intVec.get(0));
      assertTrue(intVec.isNull(1));
      assertEquals(30, intVec.get(2));
      assertTrue(intVec.isNull(3));
      assertEquals(50, intVec.get(4));
      vector.close();
    }
  }

  @Test
  public void testFullZipLayoutNullableAllValid() {
    try (BufferAllocator allocator = new RootAllocator()) {
      int numRows = 4;
      int[] values = {10, 20, 30, 40};
      byte[] valueBuffer = new byte[numRows * 4];
      ByteBuffer.wrap(valueBuffer).order(ByteOrder.LITTLE_ENDIAN)
          .putInt(values[0]).putInt(values[1]).putInt(values[2]).putInt(values[3]);
      // All valid: bitmap = 0b00001111 = 0x0F
      byte[] defBitmap = new byte[] {0x0F};

      var flat = EncodingsV21.Flat.newBuilder().setBitsPerValue(32).build();
      var valueCompression = EncodingsV21.CompressiveEncoding.newBuilder().setFlat(flat).build();
      var fullZip = EncodingsV21.FullZipLayout.newBuilder()
          .setBitsRep(0)
          .setBitsDef(1)
          .setBitsPerValue(32)
          .setNumItems(numRows)
          .setNumVisibleItems(numRows)
          .addLayers(EncodingsV21.RepDefLayer.REPDEF_NULLABLE_ITEM)
          .setValueCompression(valueCompression)
          .build();
      var pageLayout = EncodingsV21.PageLayout.newBuilder()
          .setFullZipLayout(fullZip)
          .build();

      PageBufferStore store = new PageBufferStore(List.of(defBitmap, valueBuffer));
      FullZipLayoutDecoder decoder = new FullZipLayoutDecoder();
      FieldVector vector = decoder.decode(pageLayout, numRows, store, int32Field("x"), allocator);

      assertInstanceOf(IntVector.class, vector);
      IntVector intVec = (IntVector) vector;
      assertEquals(numRows, intVec.getValueCount());
      for (int i = 0; i < numRows; i++) {
        assertEquals(values[i], intVec.get(i));
      }
      vector.close();
    }
  }

  @Test
  public void testFullZipLayoutVariableWidthThrows() {
    try (BufferAllocator allocator = new RootAllocator()) {
      var flat = EncodingsV21.Flat.newBuilder().setBitsPerValue(64).build();
      var valueCompression = EncodingsV21.CompressiveEncoding.newBuilder().setFlat(flat).build();
      var fullZip = EncodingsV21.FullZipLayout.newBuilder()
          .setBitsRep(0)
          .setBitsDef(0)
          .setBitsPerOffset(64)
          .setNumItems(4)
          .setNumVisibleItems(4)
          .setValueCompression(valueCompression)
          .build();
      var pageLayout = EncodingsV21.PageLayout.newBuilder()
          .setFullZipLayout(fullZip)
          .build();

      PageBufferStore store = new PageBufferStore(List.of(new byte[32]));
      FullZipLayoutDecoder decoder = new FullZipLayoutDecoder();
      assertThrows(UnsupportedOperationException.class, () ->
          decoder.decode(pageLayout, 4, store, int32Field("x"), allocator));
    }
  }

  @Test
  public void testFullZipLayoutGeneralZstdInt32() throws Exception {
    try (BufferAllocator allocator = new RootAllocator()) {
      int numRows = 4;
      int[] values = {100, 200, 300, 400};
      byte[] rawValues = new byte[numRows * 4];
      ByteBuffer.wrap(rawValues).order(ByteOrder.LITTLE_ENDIAN)
          .putInt(values[0]).putInt(values[1]).putInt(values[2]).putInt(values[3]);

      // Compress with zstd (Lance format: 8-byte LE size prefix + zstd frame)
      byte[] compressed = compressZstd(rawValues);

      var innerFlat = EncodingsV21.Flat.newBuilder().setBitsPerValue(32).build();
      var innerCompression = EncodingsV21.CompressiveEncoding.newBuilder().setFlat(innerFlat).build();
      var general = EncodingsV21.General.newBuilder()
          .setCompression(EncodingsV21.BufferCompression.newBuilder()
              .setScheme(EncodingsV21.CompressionScheme.COMPRESSION_ALGORITHM_ZSTD)
              .build())
          .setValues(innerCompression)
          .build();
      var valueCompression = EncodingsV21.CompressiveEncoding.newBuilder().setGeneral(general).build();
      var fullZip = EncodingsV21.FullZipLayout.newBuilder()
          .setBitsRep(0)
          .setBitsDef(0)
          .setBitsPerValue(32)
          .setNumItems(numRows)
          .setNumVisibleItems(numRows)
          .setValueCompression(valueCompression)
          .build();
      var pageLayout = EncodingsV21.PageLayout.newBuilder()
          .setFullZipLayout(fullZip)
          .build();

      PageBufferStore store = new PageBufferStore(List.of(compressed));
      FullZipLayoutDecoder decoder = new FullZipLayoutDecoder();
      FieldVector vector = decoder.decode(pageLayout, numRows, store, int32Field("x"), allocator);

      assertInstanceOf(IntVector.class, vector);
      IntVector intVec = (IntVector) vector;
      assertEquals(numRows, intVec.getValueCount());
      for (int i = 0; i < numRows; i++) {
        assertEquals(values[i], intVec.get(i));
      }
      vector.close();
    }
  }

  /** Compresses data using zstd with the Lance 8-byte LE size prefix. */
  private static byte[] compressZstd(byte[] data) {
    byte[] frame = com.github.luben.zstd.Zstd.compress(data, 3);
    byte[] result = new byte[8 + frame.length];
    ByteBuffer.wrap(result).order(ByteOrder.LITTLE_ENDIAN).putLong(data.length);
    System.arraycopy(frame, 0, result, 8, frame.length);
    return result;
  }
}
