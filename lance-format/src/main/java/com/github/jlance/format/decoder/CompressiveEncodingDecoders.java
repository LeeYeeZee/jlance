package com.github.jlance.format.decoder;

import com.github.jlance.format.buffer.PageBufferStore;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import lance.encodings21.EncodingsV21;
import lance.encodings21.EncodingsV21.CompressiveEncoding;
import lance.encodings21.EncodingsV21.Constant;
import lance.encodings21.EncodingsV21.Flat;
import lance.encodings21.EncodingsV21.General;
import lance.encodings21.EncodingsV21.Rle;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;

/**
 * Decodes a Lance V2.1+ {@link CompressiveEncoding} tree into decompressed raw bytes.
 *
 * <p>A {@code CompressiveEncoding} forms a tree whose leaves consume buffers from the
 * {@link PageBufferStore} in order. This decoder walks the tree, reading and decompressing
 * buffers to produce a little-endian {@link ByteBuffer} of raw fixed-width values.
 *
 * <p>Currently supports:
 * <ul>
 *   <li>{@code Flat} — fixed-width values in a single buffer, optionally zstd-compressed</li>
 *   <li>{@code General(zstd/lz4)} — wrapped compression around an inner encoding</li>
 *   <li>{@code Constant} — repeated constant value</li>
 *   <li>{@code Dictionary} — dictionary-encoded values with indices</li>
 * </ul>
 */
public final class CompressiveEncodingDecoders {

  private CompressiveEncodingDecoders() {}

  /**
   * Decodes the compressive encoding tree into raw little-endian bytes.
   *
   * @param encoding the compressive encoding tree root
   * @param numValues number of values expected in the output
   * @param store buffer store containing all page buffers (consumed in order)
   * @param allocator memory allocator for temporary buffers
   * @return the decompressed raw bytes as a little-endian ByteBuffer
   */
  public static ByteBuffer decode(
      CompressiveEncoding encoding,
      int numValues,
      PageBufferStore store,
      BufferAllocator allocator) {
    return switch (encoding.getCompressionCase()) {
      case FLAT -> decodeFlat(encoding.getFlat(), store);
      case GENERAL -> decodeGeneral(encoding.getGeneral(), numValues, store, allocator);
      case CONSTANT -> decodeConstant(encoding.getConstant(), numValues);
      case DICTIONARY -> throw new UnsupportedOperationException(
          "Dictionary encoding cannot be decoded to raw bytes; use decodeToVector() instead");
      case RLE -> decodeRle(encoding.getRle(), numValues, store);
      case INLINE_BITPACKING -> InlineBitpackingDecoder.decodeToBuffer(
          encoding.getInlineBitpacking(), numValues, store);
      case OUT_OF_LINE_BITPACKING -> decodeOutOfLineBitpacking(
          encoding.getOutOfLineBitpacking(), numValues, store);
      case PACKED_STRUCT -> decodePackedStruct(
          encoding.getPackedStruct(), numValues, store, allocator);
      default -> throw new UnsupportedOperationException(
          "Unsupported compressive encoding: " + encoding.getCompressionCase());
    };
  }

  private static ByteBuffer decodeFlat(Flat flat, PageBufferStore store) {
    byte[] data = store.takeNextBuffer();
    if (flat.hasData()) {
      data = CompressionUtils.maybeDecompress(data, flat.getData().getScheme());
    }
    return ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
  }

  private static ByteBuffer decodeGeneral(
      General general, int numValues, PageBufferStore store, BufferAllocator allocator) {
    byte[] compressed = store.takeNextBuffer();
    byte[] decompressed =
        CompressionUtils.maybeDecompress(compressed, general.getCompression().getScheme());
    PageBufferStore tempStore = new PageBufferStore(List.of(decompressed));
    return decode(general.getValues(), numValues, tempStore, allocator);
  }

  private static ByteBuffer decodeConstant(Constant constant, int numValues) {
    byte[] value = constant.hasValue() ? constant.getValue().toByteArray() : new byte[0];
    if (numValues == 0 || value.length == 0) {
      return ByteBuffer.wrap(new byte[0]).order(ByteOrder.LITTLE_ENDIAN);
    }
    byte[] result = new byte[value.length * numValues];
    for (int i = 0; i < numValues; i++) {
      System.arraycopy(value, 0, result, i * value.length, value.length);
    }
    return ByteBuffer.wrap(result).order(ByteOrder.LITTLE_ENDIAN);
  }

  /**
   * Decodes the compressive encoding tree into an Arrow {@link FieldVector}.
   *
   * <p>For {@code Flat}, {@code General}, and {@code Constant} this is equivalent to
   * {@link #decode(ByteBuffer, int, PageBufferStore, BufferAllocator)} followed by
   * {@link FixedWidthVectorBuilder#build}. For {@code Dictionary} it recursively decodes
   * the items and indices arrays and expands the indices into the final vector.
   *
   * @param encoding the compressive encoding tree root
   * @param numValues number of values expected in the output
   * @param store buffer store containing all page buffers (consumed in order)
   * @param field the Arrow field descriptor for the output vector
   * @param allocator memory allocator for temporary buffers
   * @return the populated Arrow vector
   */
  public static FieldVector decodeToVector(
      CompressiveEncoding encoding,
      int numValues,
      PageBufferStore store,
      Field field,
      BufferAllocator allocator) {
    return switch (encoding.getCompressionCase()) {
      case FLAT, GENERAL, CONSTANT -> {
        ByteBuffer buf = decode(encoding, numValues, store, allocator);
        int bitsPerValue = extractBitsPerValue(encoding);
        yield FixedWidthVectorBuilder.build(field, numValues, buf, bitsPerValue, allocator);
      }
      case DICTIONARY -> decodeDictionary(encoding.getDictionary(), numValues, store, field, allocator);
      case VARIABLE -> decodeVariableToVector(encoding.getVariable(), numValues, store, field, allocator);
      case RLE -> {
        ByteBuffer buf = decodeRle(encoding.getRle(), numValues, store);
        int bitsPerValue = (int) encoding.getRle().getValues().getFlat().getBitsPerValue();
        yield FixedWidthVectorBuilder.build(field, numValues, buf, bitsPerValue, allocator);
      }
      case INLINE_BITPACKING -> InlineBitpackingDecoder.decode(
          encoding.getInlineBitpacking(), numValues, store, field, allocator);
      case OUT_OF_LINE_BITPACKING -> {
        ByteBuffer buf = decodeOutOfLineBitpacking(
            encoding.getOutOfLineBitpacking(), numValues, store);
        int bitsPerValue = (int) encoding.getOutOfLineBitpacking().getUncompressedBitsPerValue();
        yield FixedWidthVectorBuilder.build(field, numValues, buf, bitsPerValue, allocator);
      }
      default -> throw new UnsupportedOperationException(
          "Unsupported compressive encoding for vector decode: " + encoding.getCompressionCase());
    };
  }

  private static ByteBuffer decodeRle(Rle rle, int numValues, PageBufferStore store) {
    byte[] data = store.takeNextBuffer();
    if (data.length < 8) {
      throw new IllegalArgumentException(
          "RLE buffer too small to contain header: " + data.length);
    }
    long valuesSizeLong =
        ((long) (data[0] & 0xFF))
            | ((long) (data[1] & 0xFF) << 8)
            | ((long) (data[2] & 0xFF) << 16)
            | ((long) (data[3] & 0xFF) << 24)
            | ((long) (data[4] & 0xFF) << 32)
            | ((long) (data[5] & 0xFF) << 40)
            | ((long) (data[6] & 0xFF) << 48)
            | ((long) (data[7] & 0xFF) << 56);
    if (valuesSizeLong < 0 || valuesSizeLong > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("RLE values size too large: " + valuesSizeLong);
    }
    int valuesSize = (int) valuesSizeLong;
    int headerSize = 8;
    if (data.length < headerSize + valuesSize) {
      throw new IllegalArgumentException(
          "RLE buffer too small: need " + (headerSize + valuesSize) + " have " + data.length);
    }

    int bitsPerValue = (int) rle.getValues().getFlat().getBitsPerValue();
    if (bitsPerValue % 8 != 0) {
      throw new UnsupportedOperationException("RLE only supports byte-aligned values");
    }
    int bytesPerValue = bitsPerValue / 8;

    if (valuesSize % bytesPerValue != 0) {
      throw new IllegalArgumentException(
          "RLE values buffer size not aligned to value width: " + valuesSize);
    }
    int numRuns = valuesSize / bytesPerValue;
    int lengthsSize = data.length - headerSize - valuesSize;
    if (lengthsSize != numRuns) {
      throw new IllegalArgumentException(
          "RLE lengths count mismatch: " + lengthsSize + " vs " + numRuns);
    }

    ByteBuffer valuesBuf =
        ByteBuffer.wrap(data, headerSize, valuesSize).order(ByteOrder.LITTLE_ENDIAN);
    byte[] lengths = java.util.Arrays.copyOfRange(data, headerSize + valuesSize, data.length);

    return expandRle(valuesBuf, lengths, numValues, bytesPerValue);
  }

  /**
   * Decodes RLE data from separate values and run-lengths buffers (V2.1 MiniBlockLayout).
   */
  static ByteBuffer decodeRleMiniBlock(
      byte[] valuesData, byte[] lengthsData, int numValues, int bitsPerValue) {
    if (bitsPerValue % 8 != 0) {
      throw new UnsupportedOperationException("RLE only supports byte-aligned values");
    }
    int bytesPerValue = bitsPerValue / 8;

    if (valuesData.length % bytesPerValue != 0) {
      throw new IllegalArgumentException(
          "RLE values buffer size not aligned to value width: " + valuesData.length);
    }
    int numRuns = valuesData.length / bytesPerValue;
    if (lengthsData.length != numRuns) {
      throw new IllegalArgumentException(
          "RLE lengths count mismatch: " + lengthsData.length + " vs " + numRuns);
    }

    ByteBuffer valuesBuf = ByteBuffer.wrap(valuesData).order(ByteOrder.LITTLE_ENDIAN);
    return expandRle(valuesBuf, lengthsData, numValues, bytesPerValue);
  }

  private static ByteBuffer expandRle(
      ByteBuffer valuesBuf, byte[] lengths, int numValues, int bytesPerValue) {
    byte[] result = new byte[numValues * bytesPerValue];
    ByteBuffer out = ByteBuffer.wrap(result).order(ByteOrder.LITTLE_ENDIAN);

    int numRuns = lengths.length;
    int valuesDecoded = 0;
    for (int i = 0; i < numRuns && valuesDecoded < numValues; i++) {
      long value = readValueAt(valuesBuf, i, bytesPerValue);
      int runLen = lengths[i] & 0xFF;
      int remaining = numValues - valuesDecoded;
      int writeLen = Math.min(runLen, remaining);
      for (int j = 0; j < writeLen; j++) {
        writeValue(out, value, bytesPerValue);
      }
      valuesDecoded += writeLen;
    }

    if (valuesDecoded != numValues) {
      throw new IllegalArgumentException(
          "RLE decoding produced " + valuesDecoded + " values, expected " + numValues);
    }

    return ByteBuffer.wrap(result).order(ByteOrder.LITTLE_ENDIAN);
  }

  private static long readValueAt(ByteBuffer buf, int index, int bytesPerValue) {
    int offset = buf.position() + index * bytesPerValue;
    return switch (bytesPerValue) {
      case 1 -> buf.get(offset) & 0xFFL;
      case 2 -> buf.getShort(offset) & 0xFFFFL;
      case 4 -> buf.getInt(offset) & 0xFFFFFFFFL;
      case 8 -> buf.getLong(offset);
      default -> throw new UnsupportedOperationException(
          "Unsupported bytesPerValue: " + bytesPerValue);
    };
  }

  private static void writeValue(ByteBuffer buf, long value, int bytesPerValue) {
    switch (bytesPerValue) {
      case 1 -> buf.put((byte) value);
      case 2 -> buf.putShort((short) value);
      case 4 -> buf.putInt((int) value);
      case 8 -> buf.putLong(value);
      default -> throw new UnsupportedOperationException(
          "Unsupported bytesPerValue: " + bytesPerValue);
    }
  }

  private static FieldVector decodeDictionary(
      EncodingsV21.Dictionary dict, int numRows,
      PageBufferStore store, Field field, BufferAllocator allocator) {
    int numItems = dict.getNumDictionaryItems();

    // Decode dictionary items
    ByteBuffer itemsBuf = decode(dict.getItems(), numItems, store, allocator);
    int itemBitsPerValue = extractBitsPerValue(dict.getItems());
    FieldVector dictValues = FixedWidthVectorBuilder.build(
        field, numItems, itemsBuf, itemBitsPerValue, allocator);

    // Decode indices
    ByteBuffer indicesBuf = decode(dict.getIndices(), numRows, store, allocator);
    int indexBitsPerValue = extractBitsPerValue(dict.getIndices());
    FieldVector indicesVec = FixedWidthVectorBuilder.build(
        createIndexField(indexBitsPerValue), numRows, indicesBuf, indexBitsPerValue, allocator);

    // Expand indices into result vector
    FieldVector result = field.createVector(allocator);
    result.allocateNew();
    TransferPair transferPair = dictValues.makeTransferPair(result);
    for (int i = 0; i < numRows; i++) {
      int idx = readIndex(indicesVec, i);
      transferPair.copyValueSafe(idx, i);
    }
    result.setValueCount(numRows);
    dictValues.close();
    indicesVec.close();
    return result;
  }

  static Field createIndexField(int bitsPerValue) {
    return new Field("idx", FieldType.nullable(new ArrowType.Int(bitsPerValue, true)), null);
  }

  static int readIndex(FieldVector indexVec, int i) {
    if (indexVec instanceof IntVector) {
      return ((IntVector) indexVec).get(i);
    }
    if (indexVec instanceof BigIntVector) {
      return (int) ((BigIntVector) indexVec).get(i);
    }
    if (indexVec instanceof SmallIntVector) {
      return ((SmallIntVector) indexVec).get(i);
    }
    if (indexVec instanceof TinyIntVector) {
      return ((TinyIntVector) indexVec).get(i);
    }
    if (indexVec instanceof UInt1Vector) {
      return ((UInt1Vector) indexVec).get(i) & 0xFF;
    }
    if (indexVec instanceof UInt2Vector) {
      return ((UInt2Vector) indexVec).get(i);
    }
    if (indexVec instanceof UInt4Vector) {
      return ((UInt4Vector) indexVec).get(i);
    }
    throw new UnsupportedOperationException(
        "Unsupported index vector type: " + indexVec.getClass().getName());
  }

  private static FieldVector decodeVariableToVector(
      EncodingsV21.Variable variable, int numValues,
      PageBufferStore store, Field field, BufferAllocator allocator) {
    int numOffsets = numValues + 1;
    CompressiveEncoding offsetsEncoding = variable.getOffsets();

    int[] offsets;
    byte[] values;

    if (offsetsEncoding.getCompressionCase()
        == lance.encodings21.EncodingsV21.CompressiveEncoding.CompressionCase.FLAT) {
      // Fast path: FLAT offsets (most common in MiniBlockLayout and FullZipLayout).
      byte[] data = store.takeNextBuffer();
      if (offsetsEncoding.getFlat().hasData()) {
        data = CompressionUtils.maybeDecompress(
            data, offsetsEncoding.getFlat().getData().getScheme());
      }
      int bitsPerOffset = (int) offsetsEncoding.getFlat().getBitsPerValue();
      if (bitsPerOffset % 8 != 0) {
        throw new UnsupportedOperationException(
            "Variable encoding offsets must be byte-aligned, got: " + bitsPerOffset);
      }
      int bytesPerOffset = bitsPerOffset / 8;
      int offsetsSize = numOffsets * bytesPerOffset;

      ByteBuffer bb = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);

      // Detect V2.0 FullZipLayout header: [bitsPerOffset: int32][valuesStartOffset: int32]
      // The valuesStartOffset should equal 8 + offsetsSize.
      boolean isV20Format = false;
      if (data.length >= 8) {
        int headerBitsPerOffset = bb.getInt(0);
        int valuesStartOffset = bb.getInt(4);
        if (headerBitsPerOffset == bitsPerOffset
            && valuesStartOffset == 8 + offsetsSize) {
          isV20Format = true;
        }
      }

      offsets = new int[numOffsets];
      if (isV20Format) {
        // V2.0 format: skip 8-byte header, read offsets, then values.
        bb.position(8);
        for (int i = 0; i < numOffsets; i++) {
          offsets[i] = bb.getInt();
        }
        values = java.util.Arrays.copyOfRange(data, 8 + offsetsSize, data.length);
      } else {
        // V2.1 MiniBlockLayout format: offsets start at byte 0, values follow.
        for (int i = 0; i < numOffsets; i++) {
          offsets[i] = bb.getInt();
        }
        if (offsetsSize < data.length) {
          values = java.util.Arrays.copyOfRange(data, offsetsSize, data.length);
        } else {
          try {
            values = store.takeNextBuffer();
          } catch (IllegalStateException e) {
            values = new byte[0];
          }
        }
      }
    } else {
      // General path: recursively decode offsets via decodeToVector.
      Field offsetsField = new Field(
          "offsets",
          new org.apache.arrow.vector.types.pojo.FieldType(
              true, new org.apache.arrow.vector.types.pojo.ArrowType.Int(32, false), null),
          null);
      FieldVector offsetsVector = decodeToVector(
          offsetsEncoding, numOffsets, store, offsetsField, allocator);

      offsets = new int[numOffsets];
      if (offsetsVector instanceof org.apache.arrow.vector.UInt4Vector u4vec) {
        for (int i = 0; i < numOffsets; i++) {
          offsets[i] = u4vec.get(i);
        }
      } else if (offsetsVector instanceof org.apache.arrow.vector.IntVector ivec) {
        for (int i = 0; i < numOffsets; i++) {
          offsets[i] = ivec.get(i);
        }
      } else {
        offsetsVector.close();
        throw new UnsupportedOperationException(
            "Variable encoding offsets must be 32-bit integers, got: "
                + offsetsVector.getClass().getName());
      }
      offsetsVector.close();

      try {
        values = store.takeNextBuffer();
      } catch (IllegalStateException e) {
        values = new byte[0];
      }
    }

    if (variable.hasValues()) {
      values = CompressionUtils.maybeDecompress(values, variable.getValues().getScheme());
    }

    FieldVector vector = field.createVector(allocator);
    // Offsets may be absolute (V2.1 MiniBlockLayout) or relative to values (V2.0 FullZipLayout).
    // Normalize them by subtracting the first offset so they are relative to values.
    int baseOffset = offsets[0];
    if (vector instanceof org.apache.arrow.vector.VarCharVector vec) {
      vec.allocateNew(numValues);
      for (int i = 0; i < numValues; i++) {
        int start = offsets[i] - baseOffset;
        int end = offsets[i + 1] - baseOffset;
        byte[] value = java.util.Arrays.copyOfRange(values, start, end);
        vec.setSafe(i, value);
      }
      vec.setValueCount(numValues);
      return vec;
    }
    if (vector instanceof org.apache.arrow.vector.VarBinaryVector vec) {
      vec.allocateNew(numValues);
      for (int i = 0; i < numValues; i++) {
        int start = offsets[i] - baseOffset;
        int end = offsets[i + 1] - baseOffset;
        byte[] value = java.util.Arrays.copyOfRange(values, start, end);
        vec.setSafe(i, value);
      }
      vec.setValueCount(numValues);
      return vec;
    }
    if (vector instanceof org.apache.arrow.vector.LargeVarCharVector vec) {
      vec.allocateNew(numValues);
      for (int i = 0; i < numValues; i++) {
        int start = offsets[i] - baseOffset;
        int end = offsets[i + 1] - baseOffset;
        byte[] value = java.util.Arrays.copyOfRange(values, start, end);
        vec.setSafe(i, value);
      }
      vec.setValueCount(numValues);
      return vec;
    }
    if (vector instanceof org.apache.arrow.vector.LargeVarBinaryVector vec) {
      vec.allocateNew(numValues);
      for (int i = 0; i < numValues; i++) {
        int start = offsets[i] - baseOffset;
        int end = offsets[i + 1] - baseOffset;
        byte[] value = java.util.Arrays.copyOfRange(values, start, end);
        vec.setSafe(i, value);
      }
      vec.setValueCount(numValues);
      return vec;
    }
    throw new UnsupportedOperationException(
        "Variable encoding does not support vector type: " + vector.getClass().getName());
  }

  private static ByteBuffer fieldVectorToByteBuffer(
      FieldVector vec, int numValues, int bitsPerValue) {
    int bytesPerValue = bitsPerValue / 8;
    int totalBytes = numValues * bytesPerValue;
    byte[] result = new byte[totalBytes];
    if (vec instanceof org.apache.arrow.vector.BaseValueVector baseVec) {
      org.apache.arrow.memory.ArrowBuf dataBuf = baseVec.getDataBuffer();
      dataBuf.getBytes(0, result, 0, totalBytes);
    } else {
      throw new UnsupportedOperationException(
          "Cannot extract bytes from vector: " + vec.getClass().getName());
    }
    vec.close();
    return ByteBuffer.wrap(result).order(ByteOrder.LITTLE_ENDIAN);
  }

  private static ByteBuffer decodePackedStruct(
      EncodingsV21.PackedStruct packed, int numValues,
      PageBufferStore store, BufferAllocator allocator) {
    int totalBits = 0;
    for (long bpv : packed.getBitsPerValueList()) {
      totalBits += (int) bpv;
    }
    if (totalBits % 8 != 0) {
      throw new UnsupportedOperationException(
          "PackedStruct only supports byte-aligned total bits, got: " + totalBits);
    }
    return decode(packed.getValues(), numValues, store, allocator);
  }

  private static ByteBuffer decodeOutOfLineBitpacking(
      EncodingsV21.OutOfLineBitpacking ool, int numValues, PageBufferStore store) {
    byte[] data = store.takeNextBuffer();
    int uncompressedBits = (int) ool.getUncompressedBitsPerValue();

    EncodingsV21.CompressiveEncoding valuesEnc = ool.getValues();
    if (valuesEnc.getCompressionCase() != EncodingsV21.CompressiveEncoding.CompressionCase.FLAT) {
      throw new UnsupportedOperationException(
          "OutOfLineBitpacking values encoding must be Flat, got: " + valuesEnc.getCompressionCase());
    }
    int compressedBits = (int) valuesEnc.getFlat().getBitsPerValue();

    return switch (uncompressedBits) {
      case 8 -> decodeOutOfLineU8(data, numValues, compressedBits);
      case 16 -> decodeOutOfLineU16(data, numValues, compressedBits);
      case 32 -> decodeOutOfLineU32(data, numValues, compressedBits);
      case 64 -> decodeOutOfLineU64(data, numValues, compressedBits);
      default -> throw new UnsupportedOperationException(
          "Unsupported uncompressed bits per value: " + uncompressedBits);
    };
  }

  private static ByteBuffer decodeOutOfLineU8(byte[] data, int numValues, int compressedBits) {
    int numPackedWords = data.length;
    int[] packed = new int[numPackedWords];
    for (int i = 0; i < numPackedWords; i++) {
      packed[i] = data[i] & 0xFF;
    }

    byte[] output = new byte[numValues];
    int numWholeChunks = numValues / 1024;
    int tailValues = numValues % 1024;
    int wordsPerChunk = (1024 * compressedBits + 7) / 8;

    int expectedFullWords = numWholeChunks * wordsPerChunk;
    boolean tailIsRaw = tailValues > 0 && packed.length == expectedFullWords + tailValues;

    byte[] chunkBuf = new byte[1024];

    for (int i = 0; i < numWholeChunks; i++) {
      FastLanesBitPacking.unpackU8(compressedBits, packed, i * wordsPerChunk, chunkBuf, 0);
      System.arraycopy(chunkBuf, 0, output, i * 1024, 1024);
    }

    if (tailValues > 0) {
      int outputStart = numWholeChunks * 1024;
      if (tailIsRaw) {
        for (int i = 0; i < tailValues; i++) {
          output[outputStart + i] = (byte) packed[expectedFullWords + i];
        }
      } else {
        FastLanesBitPacking.unpackU8(compressedBits, packed, expectedFullWords, chunkBuf, 0);
        System.arraycopy(chunkBuf, 0, output, outputStart, tailValues);
      }
    }

    return ByteBuffer.wrap(output).order(ByteOrder.LITTLE_ENDIAN);
  }

  private static ByteBuffer decodeOutOfLineU16(byte[] data, int numValues, int compressedBits) {
    int wordSize = 2;
    if (data.length % wordSize != 0) {
      throw new IllegalArgumentException("OOB u16 data length not aligned to word size");
    }
    int numPackedWords = data.length / wordSize;
    int[] packed = new int[numPackedWords];
    ByteBuffer bb = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < numPackedWords; i++) {
      packed[i] = bb.getShort() & 0xFFFF;
    }

    short[] output = new short[numValues];
    int numWholeChunks = numValues / 1024;
    int tailValues = numValues % 1024;
    int wordsPerChunk = (1024 * compressedBits + 15) / 16;

    int expectedFullWords = numWholeChunks * wordsPerChunk;
    boolean tailIsRaw = tailValues > 0 && packed.length == expectedFullWords + tailValues;

    short[] chunkBuf = new short[1024];

    for (int i = 0; i < numWholeChunks; i++) {
      FastLanesBitPacking.unpackU16(compressedBits, packed, i * wordsPerChunk, chunkBuf, 0);
      System.arraycopy(chunkBuf, 0, output, i * 1024, 1024);
    }

    if (tailValues > 0) {
      int outputStart = numWholeChunks * 1024;
      if (tailIsRaw) {
        for (int i = 0; i < tailValues; i++) {
          output[outputStart + i] = (short) packed[expectedFullWords + i];
        }
      } else {
        FastLanesBitPacking.unpackU16(compressedBits, packed, expectedFullWords, chunkBuf, 0);
        System.arraycopy(chunkBuf, 0, output, outputStart, tailValues);
      }
    }

    byte[] result = new byte[numValues * wordSize];
    ByteBuffer.wrap(result).order(ByteOrder.LITTLE_ENDIAN).asShortBuffer().put(output);
    return ByteBuffer.wrap(result).order(ByteOrder.LITTLE_ENDIAN);
  }

  private static ByteBuffer decodeOutOfLineU32(byte[] data, int numValues, int compressedBits) {
    int wordSize = 4;
    if (data.length % wordSize != 0) {
      throw new IllegalArgumentException("OOB u32 data length not aligned to word size");
    }
    int numPackedWords = data.length / wordSize;
    int[] packed = new int[numPackedWords];
    ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer().get(packed);

    int[] output = new int[numValues];
    int numWholeChunks = numValues / 1024;
    int tailValues = numValues % 1024;
    int wordsPerChunk = (1024 * compressedBits + 31) / 32;

    int expectedFullWords = numWholeChunks * wordsPerChunk;
    boolean tailIsRaw = tailValues > 0 && packed.length == expectedFullWords + tailValues;

    int[] chunkBuf = new int[1024];

    for (int i = 0; i < numWholeChunks; i++) {
      FastLanesBitPacking.unpackU32(compressedBits, packed, i * wordsPerChunk, chunkBuf, 0);
      System.arraycopy(chunkBuf, 0, output, i * 1024, 1024);
    }

    if (tailValues > 0) {
      int outputStart = numWholeChunks * 1024;
      if (tailIsRaw) {
        for (int i = 0; i < tailValues; i++) {
          output[outputStart + i] = packed[expectedFullWords + i];
        }
      } else {
        FastLanesBitPacking.unpackU32(compressedBits, packed, expectedFullWords, chunkBuf, 0);
        System.arraycopy(chunkBuf, 0, output, outputStart, tailValues);
      }
    }

    byte[] result = new byte[numValues * wordSize];
    ByteBuffer.wrap(result).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer().put(output);
    return ByteBuffer.wrap(result).order(ByteOrder.LITTLE_ENDIAN);
  }

  private static ByteBuffer decodeOutOfLineU64(byte[] data, int numValues, int compressedBits) {
    int wordSize = 8;
    if (data.length % wordSize != 0) {
      throw new IllegalArgumentException("OOB u64 data length not aligned to word size");
    }
    int numPackedWords = data.length / wordSize;
    long[] packed = new long[numPackedWords];
    ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).asLongBuffer().get(packed);

    long[] output = new long[numValues];
    int numWholeChunks = numValues / 1024;
    int tailValues = numValues % 1024;
    int wordsPerChunk = (1024 * compressedBits + 63) / 64;

    int expectedFullWords = numWholeChunks * wordsPerChunk;
    boolean tailIsRaw = tailValues > 0 && packed.length == expectedFullWords + tailValues;

    long[] chunkBuf = new long[1024];

    for (int i = 0; i < numWholeChunks; i++) {
      FastLanesBitPacking.unpackU64(compressedBits, packed, i * wordsPerChunk, chunkBuf, 0);
      System.arraycopy(chunkBuf, 0, output, i * 1024, 1024);
    }

    if (tailValues > 0) {
      int outputStart = numWholeChunks * 1024;
      if (tailIsRaw) {
        for (int i = 0; i < tailValues; i++) {
          output[outputStart + i] = packed[expectedFullWords + i];
        }
      } else {
        FastLanesBitPacking.unpackU64(compressedBits, packed, expectedFullWords, chunkBuf, 0);
        System.arraycopy(chunkBuf, 0, output, outputStart, tailValues);
      }
    }

    byte[] result = new byte[numValues * wordSize];
    ByteBuffer.wrap(result).order(ByteOrder.LITTLE_ENDIAN).asLongBuffer().put(output);
    return ByteBuffer.wrap(result).order(ByteOrder.LITTLE_ENDIAN);
  }

  /**
   * Extracts the bits_per_value from a compressive encoding tree.
   *
   * <p>Walks the tree looking for a {@code Flat} node and returns its bits_per_value.
   * This is useful for layout decoders (e.g. MiniBlockLayout) that do not store
   * bits_per_value directly but embed it in the value compression tree.
   *
   * @throws UnsupportedOperationException if the encoding tree does not contain a Flat node
   */
  public static int extractBitsPerValue(CompressiveEncoding encoding) {
    return switch (encoding.getCompressionCase()) {
      case FLAT -> (int) encoding.getFlat().getBitsPerValue();
      case GENERAL -> extractBitsPerValue(encoding.getGeneral().getValues());
      case CONSTANT -> {
        byte[] value = encoding.getConstant().hasValue()
            ? encoding.getConstant().getValue().toByteArray()
            : new byte[0];
        yield value.length * 8;
      }
      case INLINE_BITPACKING -> (int) encoding.getInlineBitpacking().getUncompressedBitsPerValue();
      case OUT_OF_LINE_BITPACKING -> (int) encoding.getOutOfLineBitpacking().getUncompressedBitsPerValue();
      case RLE -> extractBitsPerValue(encoding.getRle().getValues());
      case DICTIONARY -> extractBitsPerValue(encoding.getDictionary().getIndices());
      case PACKED_STRUCT -> {
        int totalBits = 0;
        for (long bpv : encoding.getPackedStruct().getBitsPerValueList()) {
          totalBits += (int) bpv;
        }
        yield totalBits;
      }
      default -> throw new UnsupportedOperationException(
          "Cannot extract bits_per_value from encoding: " + encoding.getCompressionCase());
    };
  }
}
