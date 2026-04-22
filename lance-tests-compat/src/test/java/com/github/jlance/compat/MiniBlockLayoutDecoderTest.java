package com.github.jlance.compat;

import static org.junit.jupiter.api.Assertions.*;

import com.github.jlance.format.buffer.PageBufferStore;
import com.github.jlance.format.decoder.MiniBlockLayoutDecoder;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import lance.encodings21.EncodingsV21;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for V2.1+ {@link MiniBlockLayoutDecoder}.
 */
public class MiniBlockLayoutDecoderTest {

  private static Field int32Field(String name) {
    return new Field(name, FieldType.nullable(new ArrowType.Int(32, true)), null);
  }

  private static Field float64Field(String name) {
    return new Field(name, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null);
  }

  @Test
  public void testSingleMiniBlockFlatInt32() {
    try (BufferAllocator allocator = new RootAllocator()) {
      int numRows = 4;
      int[] values = {10, 20, 30, 40};
      byte[] valueBuffer = new byte[numRows * 4];
      ByteBuffer.wrap(valueBuffer).order(ByteOrder.LITTLE_ENDIAN)
          .putInt(values[0]).putInt(values[1]).putInt(values[2]).putInt(values[3]);

      var flat = EncodingsV21.Flat.newBuilder().setBitsPerValue(32).build();
      var valueCompression = EncodingsV21.CompressiveEncoding.newBuilder().setFlat(flat).build();
      var miniBlock = EncodingsV21.MiniBlockLayout.newBuilder()
          .setValueCompression(valueCompression)
          .setNumBuffers(1)
          .setNumItems(numRows)
          .build();
      var pageLayout = EncodingsV21.PageLayout.newBuilder()
          .setMiniBlockLayout(miniBlock)
          .build();

      PageBufferStore store = new PageBufferStore(List.of(valueBuffer));
      MiniBlockLayoutDecoder decoder = new MiniBlockLayoutDecoder();
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
  public void testMultipleMiniBlocksFlatInt32() {
    try (BufferAllocator allocator = new RootAllocator()) {
      int rowsPerBlock = 2;
      int numBlocks = 3;
      int numRows = rowsPerBlock * numBlocks;
      int[] allValues = {10, 20, 30, 40, 50, 60};

      List<byte[]> buffers = new ArrayList<>();
      for (int b = 0; b < numBlocks; b++) {
        byte[] block = new byte[rowsPerBlock * 4];
        ByteBuffer.wrap(block).order(ByteOrder.LITTLE_ENDIAN)
            .putInt(allValues[b * rowsPerBlock])
            .putInt(allValues[b * rowsPerBlock + 1]);
        buffers.add(block);
      }

      var flat = EncodingsV21.Flat.newBuilder().setBitsPerValue(32).build();
      var valueCompression = EncodingsV21.CompressiveEncoding.newBuilder().setFlat(flat).build();
      var miniBlock = EncodingsV21.MiniBlockLayout.newBuilder()
          .setValueCompression(valueCompression)
          .setNumBuffers(1)
          .setNumItems(numRows)
          .build();
      var pageLayout = EncodingsV21.PageLayout.newBuilder()
          .setMiniBlockLayout(miniBlock)
          .build();

      PageBufferStore store = new PageBufferStore(buffers);
      MiniBlockLayoutDecoder decoder = new MiniBlockLayoutDecoder();
      FieldVector vector = decoder.decode(pageLayout, numRows, store, int32Field("x"), allocator);

      assertInstanceOf(IntVector.class, vector);
      IntVector intVec = (IntVector) vector;
      assertEquals(numRows, intVec.getValueCount());
      for (int i = 0; i < numRows; i++) {
        assertEquals(allValues[i], intVec.get(i));
      }
      vector.close();
    }
  }

  @Test
  public void testSingleMiniBlockGeneralZstdInt32() throws Exception {
    try (BufferAllocator allocator = new RootAllocator()) {
      int numRows = 4;
      int[] values = {100, 200, 300, 400};
      byte[] rawValues = new byte[numRows * 4];
      ByteBuffer.wrap(rawValues).order(ByteOrder.LITTLE_ENDIAN)
          .putInt(values[0]).putInt(values[1]).putInt(values[2]).putInt(values[3]);

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
      var miniBlock = EncodingsV21.MiniBlockLayout.newBuilder()
          .setValueCompression(valueCompression)
          .setNumBuffers(1)
          .setNumItems(numRows)
          .build();
      var pageLayout = EncodingsV21.PageLayout.newBuilder()
          .setMiniBlockLayout(miniBlock)
          .build();

      PageBufferStore store = new PageBufferStore(List.of(compressed));
      MiniBlockLayoutDecoder decoder = new MiniBlockLayoutDecoder();
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
  public void testMultipleMiniBlocksGeneralZstdFloat64() throws Exception {
    try (BufferAllocator allocator = new RootAllocator()) {
      int rowsPerBlock = 2;
      int numBlocks = 2;
      int numRows = rowsPerBlock * numBlocks;
      double[] allValues = {1.1, 2.2, 3.3, 4.4};

      List<byte[]> buffers = new ArrayList<>();
      for (int b = 0; b < numBlocks; b++) {
        byte[] raw = new byte[rowsPerBlock * 8];
        ByteBuffer.wrap(raw).order(ByteOrder.LITTLE_ENDIAN)
            .putDouble(allValues[b * rowsPerBlock])
            .putDouble(allValues[b * rowsPerBlock + 1]);
        buffers.add(compressZstd(raw));
      }

      var innerFlat = EncodingsV21.Flat.newBuilder().setBitsPerValue(64).build();
      var innerCompression = EncodingsV21.CompressiveEncoding.newBuilder().setFlat(innerFlat).build();
      var general = EncodingsV21.General.newBuilder()
          .setCompression(EncodingsV21.BufferCompression.newBuilder()
              .setScheme(EncodingsV21.CompressionScheme.COMPRESSION_ALGORITHM_ZSTD)
              .build())
          .setValues(innerCompression)
          .build();
      var valueCompression = EncodingsV21.CompressiveEncoding.newBuilder().setGeneral(general).build();
      var miniBlock = EncodingsV21.MiniBlockLayout.newBuilder()
          .setValueCompression(valueCompression)
          .setNumBuffers(1)
          .setNumItems(numRows)
          .build();
      var pageLayout = EncodingsV21.PageLayout.newBuilder()
          .setMiniBlockLayout(miniBlock)
          .build();

      PageBufferStore store = new PageBufferStore(buffers);
      MiniBlockLayoutDecoder decoder = new MiniBlockLayoutDecoder();
      FieldVector vector = decoder.decode(pageLayout, numRows, store, float64Field("y"), allocator);

      assertInstanceOf(Float8Vector.class, vector);
      Float8Vector floatVec = (Float8Vector) vector;
      assertEquals(numRows, floatVec.getValueCount());
      for (int i = 0; i < numRows; i++) {
        assertEquals(allValues[i], floatVec.get(i), 0.0001);
      }
      vector.close();
    }
  }

  @Test
  public void testWithRepLevels() {
    try (BufferAllocator allocator = new RootAllocator()) {
      var flat = EncodingsV21.Flat.newBuilder().setBitsPerValue(32).build();
      var valueCompression = EncodingsV21.CompressiveEncoding.newBuilder().setFlat(flat).build();
      var miniBlock = EncodingsV21.MiniBlockLayout.newBuilder()
          .setValueCompression(valueCompression)
          .setNumBuffers(1)
          .setNumItems(4)
          .setRepCompression(EncodingsV21.CompressiveEncoding.newBuilder()
              .setFlat(EncodingsV21.Flat.newBuilder().setBitsPerValue(16).build())
              .build())
          .build();
      var pageLayout = EncodingsV21.PageLayout.newBuilder()
          .setMiniBlockLayout(miniBlock)
          .build();

      PageBufferStore store = new PageBufferStore(List.of(new byte[8], new byte[16]));
      MiniBlockLayoutDecoder decoder = new MiniBlockLayoutDecoder();
      var vec = decoder.decode(pageLayout, 4, store, int32Field("x"), allocator);
      assertEquals(4, vec.getValueCount());
      vec.close();
    }
  }

  @Test
  public void testMiniBlockNullableInt32() {
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
      var miniBlock = EncodingsV21.MiniBlockLayout.newBuilder()
          .setValueCompression(valueCompression)
          .setNumBuffers(1)
          .setNumItems(numRows)
          .addLayers(EncodingsV21.RepDefLayer.REPDEF_NULLABLE_ITEM)
          .setDefCompression(EncodingsV21.CompressiveEncoding.newBuilder()
              .setFlat(EncodingsV21.Flat.newBuilder().setBitsPerValue(1).build())
              .build())
          .build();
      var pageLayout = EncodingsV21.PageLayout.newBuilder()
          .setMiniBlockLayout(miniBlock)
          .build();

      // Buffer order: def buffer, then mini-block value buffer
      PageBufferStore store = new PageBufferStore(List.of(defBitmap, valueBuffer));
      MiniBlockLayoutDecoder decoder = new MiniBlockLayoutDecoder();
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
  public void testMiniBlockNullableAllValid() {
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
      var miniBlock = EncodingsV21.MiniBlockLayout.newBuilder()
          .setValueCompression(valueCompression)
          .setNumBuffers(1)
          .setNumItems(numRows)
          .addLayers(EncodingsV21.RepDefLayer.REPDEF_NULLABLE_ITEM)
          .setDefCompression(EncodingsV21.CompressiveEncoding.newBuilder()
              .setFlat(EncodingsV21.Flat.newBuilder().setBitsPerValue(1).build())
              .build())
          .build();
      var pageLayout = EncodingsV21.PageLayout.newBuilder()
          .setMiniBlockLayout(miniBlock)
          .build();

      PageBufferStore store = new PageBufferStore(List.of(defBitmap, valueBuffer));
      MiniBlockLayoutDecoder decoder = new MiniBlockLayoutDecoder();
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
  public void testNumBuffersNotOneThrows() {
    try (BufferAllocator allocator = new RootAllocator()) {
      var flat = EncodingsV21.Flat.newBuilder().setBitsPerValue(32).build();
      var valueCompression = EncodingsV21.CompressiveEncoding.newBuilder().setFlat(flat).build();
      var miniBlock = EncodingsV21.MiniBlockLayout.newBuilder()
          .setValueCompression(valueCompression)
          .setNumBuffers(2)
          .setNumItems(4)
          .build();
      var pageLayout = EncodingsV21.PageLayout.newBuilder()
          .setMiniBlockLayout(miniBlock)
          .build();

      PageBufferStore store = new PageBufferStore(List.of(new byte[8], new byte[8]));
      MiniBlockLayoutDecoder decoder = new MiniBlockLayoutDecoder();
      assertThrows(UnsupportedOperationException.class, () ->
          decoder.decode(pageLayout, 4, store, int32Field("x"), allocator));
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
