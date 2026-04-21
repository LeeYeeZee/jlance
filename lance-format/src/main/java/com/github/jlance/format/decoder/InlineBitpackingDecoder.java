package com.github.jlance.format.decoder;

import com.github.jlance.format.buffer.PageBufferStore;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import lance.encodings21.EncodingsV21;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Decodes Lance V2.1+ {@link EncodingsV21.InlineBitpacking} (FastLanes).
 *
 * <p>InlineBitpacking stores the bit-width for each 1024-value chunk inline in the buffer.
 * The header size is 1 byte for 8-bit types and 2 bytes for wider types.
 */
public class InlineBitpackingDecoder {

  private static final int CHUNK_SIZE = 1024;

  private InlineBitpackingDecoder() {}

  /**
   * Decodes an InlineBitpacking-encoded page buffer into an Arrow vector.
   */
  public static FieldVector decode(
      EncodingsV21.InlineBitpacking encoding,
      int numValues,
      PageBufferStore store,
      Field field,
      BufferAllocator allocator) {
    ByteBuffer buf = decodeToBuffer(encoding, numValues, store);
    int uncompressedBits = (int) encoding.getUncompressedBitsPerValue();
    return FixedWidthVectorBuilder.build(field, numValues, buf, uncompressedBits, allocator);
  }

  /**
   * Decodes an InlineBitpacking-encoded page buffer into raw little-endian bytes.
   *
   * <p>If {@code numValues <= 0} the entire buffer is decoded (used by the
   * {@link CompressiveEncodingDecoders#decode} path where the caller concatenates
   * multiple mini-blocks and does not know the per-mini-block value count).
   */
  public static ByteBuffer decodeToBuffer(
      EncodingsV21.InlineBitpacking encoding,
      int numValues,
      PageBufferStore store) {
    byte[] data = store.takeNextBuffer();
    if (encoding.hasValues()) {
      data = CompressionUtils.maybeDecompress(data, encoding.getValues().getScheme());
    }

    int uncompressedBits = (int) encoding.getUncompressedBitsPerValue();
    return switch (uncompressedBits) {
      case 8 -> decodeInlineU8(data, numValues);
      case 16 -> decodeInlineU16(data, numValues);
      case 32 -> decodeInlineU32(data, numValues);
      case 64 -> decodeInlineU64(data, numValues);
      default -> throw new UnsupportedOperationException(
          "Unsupported uncompressed bits per value: " + uncompressedBits);
    };
  }

  private static ByteBuffer decodeInlineU8(byte[] data, int numValues) {
    int metadataBytes = 1;
    boolean allChunks = numValues <= 0;
    int maxChunks = allChunks ? Integer.MAX_VALUE : (numValues + CHUNK_SIZE - 1) / CHUNK_SIZE;

    ArrayList<Byte> acc = allChunks ? new ArrayList<>() : null;
    byte[] output = allChunks ? null : new byte[numValues];
    int pos = 0;
    int outPos = 0;

    for (int i = 0; i < maxChunks && pos < data.length; i++) {
      int bitWidth = data[pos] & 0xFF;
      pos += metadataBytes;

      int packedLenBytes = (bitWidth * CHUNK_SIZE) / 8;
      int[] packed = new int[packedLenBytes];
      for (int j = 0; j < packedLenBytes; j++) {
        packed[j] = data[pos + j] & 0xFF;
      }
      pos += packedLenBytes;

      byte[] chunkBuf = new byte[CHUNK_SIZE];
      FastLanesBitPacking.unpackU8(bitWidth, packed, 0, chunkBuf, 0);

      if (allChunks) {
        for (int j = 0; j < CHUNK_SIZE; j++) {
          acc.add(chunkBuf[j]);
        }
      } else {
        int copyLen = (i == maxChunks - 1) ? (numValues - i * CHUNK_SIZE) : CHUNK_SIZE;
        System.arraycopy(chunkBuf, 0, output, outPos, copyLen);
        outPos += copyLen;
      }
    }

    if (allChunks) {
      output = new byte[acc.size()];
      for (int i = 0; i < acc.size(); i++) output[i] = acc.get(i);
    }
    return ByteBuffer.wrap(output).order(ByteOrder.LITTLE_ENDIAN);
  }

  private static ByteBuffer decodeInlineU16(byte[] data, int numValues) {
    int metadataBytes = 2;
    boolean allChunks = numValues <= 0;
    int maxChunks = allChunks ? Integer.MAX_VALUE : (numValues + CHUNK_SIZE - 1) / CHUNK_SIZE;

    ArrayList<Short> acc = allChunks ? new ArrayList<>() : null;
    short[] output = allChunks ? null : new short[numValues];
    int pos = 0;
    int outPos = 0;

    for (int i = 0; i < maxChunks && pos < data.length; i++) {
      int bitWidth = ByteBuffer.wrap(data, pos, metadataBytes).order(ByteOrder.LITTLE_ENDIAN).getShort() & 0xFFFF;
      pos += metadataBytes;

      int packedLenShorts = (bitWidth * CHUNK_SIZE) / 16;
      int packedLenBytes = packedLenShorts * 2;
      int[] packed = new int[packedLenShorts];
      ByteBuffer bb = ByteBuffer.wrap(data, pos, packedLenBytes).order(ByteOrder.LITTLE_ENDIAN);
      for (int j = 0; j < packedLenShorts; j++) {
        packed[j] = bb.getShort() & 0xFFFF;
      }
      pos += packedLenBytes;

      short[] chunkBuf = new short[CHUNK_SIZE];
      FastLanesBitPacking.unpackU16(bitWidth, packed, 0, chunkBuf, 0);

      if (allChunks) {
        for (int j = 0; j < CHUNK_SIZE; j++) {
          acc.add(chunkBuf[j]);
        }
      } else {
        int copyLen = (i == maxChunks - 1) ? (numValues - i * CHUNK_SIZE) : CHUNK_SIZE;
        System.arraycopy(chunkBuf, 0, output, outPos, copyLen);
        outPos += copyLen;
      }
    }

    if (allChunks) {
      output = new short[acc.size()];
      for (int i = 0; i < acc.size(); i++) output[i] = acc.get(i);
      numValues = acc.size();
    }
    byte[] resultBytes = new byte[numValues * 2];
    ByteBuffer.wrap(resultBytes).order(ByteOrder.LITTLE_ENDIAN).asShortBuffer().put(output);
    return ByteBuffer.wrap(resultBytes).order(ByteOrder.LITTLE_ENDIAN);
  }

  private static ByteBuffer decodeInlineU32(byte[] data, int numValues) {
    int metadataBytes = 4;
    boolean allChunks = numValues <= 0;
    int maxChunks = allChunks ? Integer.MAX_VALUE : (numValues + CHUNK_SIZE - 1) / CHUNK_SIZE;

    ArrayList<Integer> acc = allChunks ? new ArrayList<>() : null;
    int[] output = allChunks ? null : new int[numValues];
    int pos = 0;
    int outPos = 0;

    for (int i = 0; i < maxChunks && pos < data.length; i++) {
      if (pos + metadataBytes > data.length) {
        break;
      }
      int bitWidth = (data[pos] & 0xFF) | ((data[pos + 1] & 0xFF) << 8);
      // decodeInlineU32 chunk parsed
      pos += metadataBytes;

      int packedLenInts = (bitWidth * CHUNK_SIZE) / 32;
      int packedLenBytes = packedLenInts * 4;
      int[] packed = new int[packedLenInts];
      if (packedLenBytes > 0) {
        ByteBuffer.wrap(data, pos, packedLenBytes).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer().get(packed);
        pos += packedLenBytes;
      }

      int[] chunkBuf = new int[CHUNK_SIZE];
      
      FastLanesBitPacking.unpackU32(bitWidth, packed, 0, chunkBuf, 0);

      if (allChunks) {
        for (int j = 0; j < CHUNK_SIZE; j++) {
          acc.add(chunkBuf[j]);
        }
      } else {
        int copyLen = (i == maxChunks - 1) ? (numValues - i * CHUNK_SIZE) : CHUNK_SIZE;
        System.arraycopy(chunkBuf, 0, output, outPos, copyLen);
        outPos += copyLen;
      }
    }

    if (allChunks) {
      output = new int[acc.size()];
      for (int i = 0; i < acc.size(); i++) output[i] = acc.get(i);
      numValues = acc.size();
    }
    byte[] resultBytes = new byte[numValues * 4];
    ByteBuffer.wrap(resultBytes).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer().put(output);
    return ByteBuffer.wrap(resultBytes).order(ByteOrder.LITTLE_ENDIAN);
  }

  private static ByteBuffer decodeInlineU64(byte[] data, int numValues) {
    int metadataBytes = 8;
    boolean allChunks = numValues <= 0;
    int maxChunks = allChunks ? Integer.MAX_VALUE : (numValues + CHUNK_SIZE - 1) / CHUNK_SIZE;

    ArrayList<Long> acc = allChunks ? new ArrayList<>() : null;
    long[] output = allChunks ? null : new long[numValues];
    int pos = 0;
    int outPos = 0;

    for (int i = 0; i < maxChunks && pos < data.length; i++) {
      if (pos + metadataBytes > data.length) {
        break;
      }
      int bitWidth = (int) ByteBuffer.wrap(data, pos, metadataBytes).order(ByteOrder.LITTLE_ENDIAN).getLong();
      pos += metadataBytes;

      int packedLenLongs = (bitWidth * CHUNK_SIZE) / 64;
      int packedLenBytes = packedLenLongs * 8;
      long[] packed = new long[packedLenLongs];
      if (packedLenBytes > 0) {
        ByteBuffer.wrap(data, pos, packedLenBytes).order(ByteOrder.LITTLE_ENDIAN).asLongBuffer().get(packed);
        pos += packedLenBytes;
      }

      long[] chunkBuf = new long[CHUNK_SIZE];
      FastLanesBitPacking.unpackU64(bitWidth, packed, 0, chunkBuf, 0);

      if (allChunks) {
        for (int j = 0; j < CHUNK_SIZE; j++) {
          acc.add(chunkBuf[j]);
        }
      } else {
        int copyLen = (i == maxChunks - 1) ? (numValues - i * CHUNK_SIZE) : CHUNK_SIZE;
        System.arraycopy(chunkBuf, 0, output, outPos, copyLen);
        outPos += copyLen;
      }
    }

    if (allChunks) {
      output = new long[acc.size()];
      for (int i = 0; i < acc.size(); i++) output[i] = acc.get(i);
      numValues = acc.size();
    }
    byte[] resultBytes = new byte[numValues * 8];
    ByteBuffer.wrap(resultBytes).order(ByteOrder.LITTLE_ENDIAN).asLongBuffer().put(output);
    return ByteBuffer.wrap(resultBytes).order(ByteOrder.LITTLE_ENDIAN);
  }
}
