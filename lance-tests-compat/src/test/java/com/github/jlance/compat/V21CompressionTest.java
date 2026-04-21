package com.github.jlance.compat;

import static org.junit.jupiter.api.Assertions.*;

import com.github.jlance.format.buffer.PageBufferStore;
import com.github.jlance.format.decoder.FullZipLayoutDecoder;
import com.github.jlance.format.decoder.MiniBlockLayoutDecoder;
import java.io.ByteArrayOutputStream;
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
 * Unit tests for V2.1+ compression paths (zstd and lz4).
 */
public class V21CompressionTest {

  private static Field int32Field(String name) {
    return new Field(name, FieldType.nullable(new ArrowType.Int(32, true)), null);
  }

  // ==================== Flat.data compression tests ====================

  @Test
  public void testFlatWithZstdDataCompression() throws Exception {
    try (BufferAllocator allocator = new RootAllocator()) {
      int numRows = 4;
      int[] values = {10, 20, 30, 40};
      byte[] raw = new byte[numRows * 4];
      ByteBuffer.wrap(raw).order(ByteOrder.LITTLE_ENDIAN)
          .putInt(values[0]).putInt(values[1]).putInt(values[2]).putInt(values[3]);

      byte[] compressed = compressZstd(raw);

      var flat = EncodingsV21.Flat.newBuilder()
          .setBitsPerValue(32)
          .setData(EncodingsV21.BufferCompression.newBuilder()
              .setScheme(EncodingsV21.CompressionScheme.COMPRESSION_ALGORITHM_ZSTD)
              .build())
          .build();
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

  @Test
  public void testFlatWithLz4DataCompression() throws Exception {
    try (BufferAllocator allocator = new RootAllocator()) {
      int numRows = 4;
      int[] values = {11, 22, 33, 44};
      byte[] raw = new byte[numRows * 4];
      ByteBuffer.wrap(raw).order(ByteOrder.LITTLE_ENDIAN)
          .putInt(values[0]).putInt(values[1]).putInt(values[2]).putInt(values[3]);

      byte[] compressed = compressLz4(raw);

      var flat = EncodingsV21.Flat.newBuilder()
          .setBitsPerValue(32)
          .setData(EncodingsV21.BufferCompression.newBuilder()
              .setScheme(EncodingsV21.CompressionScheme.COMPRESSION_ALGORITHM_LZ4)
              .build())
          .build();
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

  // ==================== General compression tests ====================

  @Test
  public void testGeneralLz4Int32() throws Exception {
    try (BufferAllocator allocator = new RootAllocator()) {
      int numRows = 4;
      int[] values = {100, 200, 300, 400};
      byte[] raw = new byte[numRows * 4];
      ByteBuffer.wrap(raw).order(ByteOrder.LITTLE_ENDIAN)
          .putInt(values[0]).putInt(values[1]).putInt(values[2]).putInt(values[3]);

      byte[] compressed = compressLz4(raw);

      var innerFlat = EncodingsV21.Flat.newBuilder().setBitsPerValue(32).build();
      var innerCompression = EncodingsV21.CompressiveEncoding.newBuilder().setFlat(innerFlat).build();
      var general = EncodingsV21.General.newBuilder()
          .setCompression(EncodingsV21.BufferCompression.newBuilder()
              .setScheme(EncodingsV21.CompressionScheme.COMPRESSION_ALGORITHM_LZ4)
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

  // ==================== Compression + nullable tests ====================

  @Test
  public void testMiniBlockNullableWithLz4DefCompression() throws Exception {
    try (BufferAllocator allocator = new RootAllocator()) {
      int numRows = 5;
      int[] visibleValues = {10, 30, 50};
      byte[] valueBuffer = new byte[visibleValues.length * 4];
      ByteBuffer.wrap(valueBuffer).order(ByteOrder.LITTLE_ENDIAN)
          .putInt(visibleValues[0]).putInt(visibleValues[1]).putInt(visibleValues[2]);
      // Validity: rows 0,2,4 valid; rows 1,3 null → bitmap = 0b00010101 = 0x15
      byte[] defBitmap = new byte[] {0x15};
      byte[] defCompressed = compressLz4(defBitmap);

      var flat = EncodingsV21.Flat.newBuilder().setBitsPerValue(32).build();
      var valueCompression = EncodingsV21.CompressiveEncoding.newBuilder().setFlat(flat).build();
      var miniBlock = EncodingsV21.MiniBlockLayout.newBuilder()
          .setValueCompression(valueCompression)
          .setNumBuffers(1)
          .setNumItems(numRows)
          .addLayers(EncodingsV21.RepDefLayer.REPDEF_NULLABLE_ITEM)
          .setDefCompression(EncodingsV21.CompressiveEncoding.newBuilder()
              .setFlat(EncodingsV21.Flat.newBuilder()
                  .setBitsPerValue(1)
                  .setData(EncodingsV21.BufferCompression.newBuilder()
                      .setScheme(EncodingsV21.CompressionScheme.COMPRESSION_ALGORITHM_LZ4)
                      .build())
                  .build())
              .build())
          .build();
      var pageLayout = EncodingsV21.PageLayout.newBuilder()
          .setMiniBlockLayout(miniBlock)
          .build();

      // Buffer order: def buffer (compressed), then mini-block value buffer
      PageBufferStore store = new PageBufferStore(List.of(defCompressed, valueBuffer));
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
  public void testMiniBlockNullableWithZstdDefCompression() throws Exception {
    try (BufferAllocator allocator = new RootAllocator()) {
      int numRows = 5;
      int[] visibleValues = {10, 30, 50};
      byte[] valueBuffer = new byte[visibleValues.length * 4];
      ByteBuffer.wrap(valueBuffer).order(ByteOrder.LITTLE_ENDIAN)
          .putInt(visibleValues[0]).putInt(visibleValues[1]).putInt(visibleValues[2]);
      byte[] defBitmap = new byte[] {0x15};
      byte[] defCompressed = compressZstd(defBitmap);

      var flat = EncodingsV21.Flat.newBuilder().setBitsPerValue(32).build();
      var valueCompression = EncodingsV21.CompressiveEncoding.newBuilder().setFlat(flat).build();
      var miniBlock = EncodingsV21.MiniBlockLayout.newBuilder()
          .setValueCompression(valueCompression)
          .setNumBuffers(1)
          .setNumItems(numRows)
          .addLayers(EncodingsV21.RepDefLayer.REPDEF_NULLABLE_ITEM)
          .setDefCompression(EncodingsV21.CompressiveEncoding.newBuilder()
              .setFlat(EncodingsV21.Flat.newBuilder()
                  .setBitsPerValue(1)
                  .setData(EncodingsV21.BufferCompression.newBuilder()
                      .setScheme(EncodingsV21.CompressionScheme.COMPRESSION_ALGORITHM_ZSTD)
                      .build())
                  .build())
              .build())
          .build();
      var pageLayout = EncodingsV21.PageLayout.newBuilder()
          .setMiniBlockLayout(miniBlock)
          .build();

      PageBufferStore store = new PageBufferStore(List.of(defCompressed, valueBuffer));
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

  // ==================== Helper methods ====================

  /** Compresses data using zstd with the Lance 8-byte LE size prefix. */
  private static byte[] compressZstd(byte[] data) {
    byte[] frame = com.github.luben.zstd.Zstd.compress(data, 3);
    byte[] result = new byte[8 + frame.length];
    ByteBuffer.wrap(result).order(ByteOrder.LITTLE_ENDIAN).putLong(data.length);
    System.arraycopy(frame, 0, result, 8, frame.length);
    return result;
  }

  /** Compresses data using LZ4 frame format with the Lance 8-byte LE size prefix. */
  private static byte[] compressLz4(byte[] data) throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ByteBuffer.wrap(baos.toByteArray()); // placeholder to ensure import
    baos.write(new byte[8]); // size prefix placeholder
    try (net.jpountz.lz4.LZ4FrameOutputStream lz4os =
             new net.jpountz.lz4.LZ4FrameOutputStream(baos)) {
      lz4os.write(data);
    }
    byte[] result = baos.toByteArray();
    // Write actual size into the prefix
    ByteBuffer.wrap(result).order(ByteOrder.LITTLE_ENDIAN).putLong(data.length);
    return result;
  }
}
