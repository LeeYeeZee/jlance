// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.format.decoder;

import com.github.jlance.format.buffer.PageBufferStore;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lance.encodings21.EncodingsV21.MiniBlockLayout;
import lance.encodings21.EncodingsV21.PageLayout;
import lance.encodings21.EncodingsV21.RepDefLayer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Decodes a {@link MiniBlockLayout} into an Arrow vector.
 *
 * <p>MiniBlockLayout is the default structural layout for small types. Data is compressed
 * into small chunks (mini-blocks) which are roughly the size of a disk sector.
 *
 * <p>Currently supports:
 * <ul>
 *   <li>No rep/def layers or a single {@code NULLABLE_ITEM} layer</li>
 *   <li>No dictionary</li>
 *   <li>Fixed-width values (extracted from the value compression tree)</li>
 *   <li>{@code Flat} or {@code General(zstd)} value compression</li>
 * </ul>
 */
public class MiniBlockLayoutDecoder implements PageLayoutDecoder {

  @Override
  public DecodedArray decodeWithRepDef(
      PageLayout layout,
      int numRows,
      PageBufferStore store,
      Field field,
      BufferAllocator allocator) {
    var miniBlock = layout.getMiniBlockLayout();
    List<RepDefLayer> layers = miniBlock.getLayersList();

    // Pre-extract rep/def levels before the decode() path consumes buffers.
    short[] repLevels = null;
    short[] defLevels = null;
    if (miniBlock.hasRepCompression() || hasRepDefLayers(layers)) {
      try {
        repLevels = extractRepetitionLevels(layout, numRows, store, allocator);
      } catch (Exception e) {
        // Ignore extraction failures for rep levels
      }
      try {
        defLevels = extractDefinitionLevels(layout, numRows, store, allocator);
      } catch (Exception e) {
        // Ignore extraction failures for def levels
      }
    }

    FieldVector vector = decode(layout, numRows, store, field, allocator);
    return new DecodedArray(vector, repLevels, defLevels, layers);
  }

  private static boolean hasRepDefLayers(List<RepDefLayer> layers) {
    return layers.stream().anyMatch(l ->
        l == RepDefLayer.REPDEF_NULLABLE_ITEM
            || l == RepDefLayer.REPDEF_NULLABLE_LIST
            || l == RepDefLayer.REPDEF_EMPTYABLE_LIST
            || l == RepDefLayer.REPDEF_NULL_AND_EMPTY_LIST);
  }

  public FieldVector decode(
      PageLayout layout,
      int numRows,
      PageBufferStore store,
      Field field,
      BufferAllocator allocator) {
    var miniBlock = layout.getMiniBlockLayout();

    List<RepDefLayer> layers = miniBlock.getLayersList();
    if (!isSupportedLayer(layers)) {
      throw new UnsupportedOperationException(
          "MiniBlockLayout with unsupported rep/def layers: " + layers);
    }

    boolean hasNullableItem = layers.stream()
        .anyMatch(l -> l == RepDefLayer.REPDEF_NULLABLE_ITEM);
    int nullableLayerCount = (int) layers.stream()
        .filter(l -> l == RepDefLayer.REPDEF_NULLABLE_ITEM).count();
    int maxDefLevel = nullableLayerCount;
    boolean hasListLayer = hasListLayer(layers);

    boolean hasDictionary = miniBlock.hasDictionary();

    boolean isVariableWidth = miniBlock.getValueCompression().getCompressionCase()
        == lance.encodings21.EncodingsV21.CompressiveEncoding.CompressionCase.VARIABLE;
    int bitsPerValue = -1;
    if (!isVariableWidth) {
      bitsPerValue = CompressiveEncodingDecoders.extractBitsPerValue(
          miniBlock.getValueCompression());
      if (bitsPerValue % 8 != 0) {
        throw new UnsupportedOperationException(
            "MiniBlockLayout only supports byte-aligned bits_per_value, got: " + bitsPerValue);
      }
    }

    long numBuffersPerMiniBlock = miniBlock.getNumBuffers();
    if (numBuffersPerMiniBlock < 1) {
      throw new UnsupportedOperationException(
          "MiniBlockLayout with num_buffers < 1 not supported, got: " + numBuffersPerMiniBlock);
    }
    if (numBuffersPerMiniBlock > 1
        && miniBlock.getValueCompression().getCompressionCase()
            != lance.encodings21.EncodingsV21.CompressiveEncoding.CompressionCase.RLE) {
      throw new UnsupportedOperationException(
          "MiniBlockLayout with num_buffers > 1 only supported for RLE value compression");
    }

    // Determine whether we have real Lance V2.1 chunk metadata or legacy flat buffers.
    // Real files have two possible layouts:
    //   Case A: separate metadata buffer (2 bytes per chunk) + data buffer (chunks)
    //   Case B: metadata + data inside the first page buffer (padded or followed by other data)
    // Legacy unit tests pass each mini-block as a separate buffer.
    int remainingBuffers = store.getBufferCount() - store.getCurrentBufferIndex();
    boolean newFormat = false;
    boolean newFormatSeparateBuffers = false;
    int[] chunkSizes = null;
    int[] logNumValues = null;
    int numChunks = 0;

    boolean hasLargeChunk = miniBlock.getHasLargeChunk();
    int metaWordSize = hasLargeChunk ? 4 : 2;

    // Try Case A: separate buffers
    if (remainingBuffers >= 2) {
      byte[] candidateMeta = store.getBuffer(store.getCurrentBufferIndex());
      byte[] candidateData = store.getBuffer(store.getCurrentBufferIndex() + 1);

      if (candidateMeta.length % metaWordSize == 0) {
        int candidateChunks = candidateMeta.length / metaWordSize;
        int totalChunkSize = 0;
        for (int i = 0; i < candidateChunks; i++) {
          int word = readMetaWord(candidateMeta, i * metaWordSize, metaWordSize);
          int dividedBytes = word >>> 4;
          totalChunkSize += (dividedBytes + 1) * 8;
        }

        if (totalChunkSize == candidateData.length) {
          // Sanity check: candidateData should start with a chunk header where numLevels is reasonable
          if (candidateData.length >= 2) {
            int numLevels = (candidateData[0] & 0xFF) | ((candidateData[1] & 0xFF) << 8);
            // candidate data sanity check passed
            if (numLevels >= 0 && numLevels <= Math.max(numRows, 1024)) {
              newFormat = true;
              newFormatSeparateBuffers = true;
            }
          }
          if (newFormat) {
            numChunks = candidateChunks;
            chunkSizes = new int[numChunks];
            logNumValues = new int[numChunks];
            for (int i = 0; i < numChunks; i++) {
              int word = readMetaWord(candidateMeta, i * metaWordSize, metaWordSize);
              logNumValues[i] = word & 0x0F;
              int dividedBytes = word >>> 4;
              chunkSizes[i] = (dividedBytes + 1) * 8;
            }
          }
        }
      }
    }

    // Try Case B: single buffer
    if (!newFormat && remainingBuffers >= 1) {
      byte[] firstBuffer = store.getBuffer(store.getCurrentBufferIndex());
      if (firstBuffer.length >= metaWordSize) {
        int offset = 0;
        int totalChunkSize = 0;
        int candidateChunks = 0;
        while (offset + metaWordSize <= firstBuffer.length) {
          int word = readMetaWord(firstBuffer, offset, metaWordSize);
          int dividedBytes = word >>> 4;
          int chunkSize = (dividedBytes + 1) * 8;
          if (offset + metaWordSize + chunkSize > firstBuffer.length) {
            break;
          }
          offset += metaWordSize;
          totalChunkSize += chunkSize;
          candidateChunks++;
          if (candidateChunks > 4096) {
            break;
          }
        }
        if (candidateChunks > 0
            && metaWordSize * candidateChunks + totalChunkSize <= firstBuffer.length) {
          newFormat = true;
          numChunks = candidateChunks;
          chunkSizes = new int[numChunks];
          logNumValues = new int[numChunks];
          offset = 0;
          for (int i = 0; i < numChunks; i++) {
            int word = readMetaWord(firstBuffer, offset, metaWordSize);
            logNumValues[i] = word & 0x0F;
            chunkSizes[i] = ((word >>> 4) + 1) * 8;
            offset += metaWordSize;
          }
        }
      }
    }

    List<ByteBuffer> miniBlockData = new ArrayList<>();
    int totalBytes = 0;
    List<FieldVector> miniBlockVectors = new ArrayList<>();

    List<boolean[]> chunkValidities = new ArrayList<>();

    if (newFormat) {
      byte[] metaBuffer;
      byte[] dataBuffer;
      if (newFormatSeparateBuffers) {
        metaBuffer = store.takeNextBuffer();
        dataBuffer = store.takeNextBuffer();
      } else {
        byte[] pageBuffer = store.takeNextBuffer();
        int totalChunkSize = 0;
        for (int s : chunkSizes) {
          totalChunkSize += s;
        }
        metaBuffer = Arrays.copyOfRange(pageBuffer, 0, 2 * numChunks);
        dataBuffer = Arrays.copyOfRange(pageBuffer, 2 * numChunks, 2 * numChunks + totalChunkSize);
      }

      int dataOffset = 0;
      long valsInPrevChunks = 0;
      int numBuffers = (int) numBuffersPerMiniBlock;

      for (int i = 0; i < numChunks; i++) {
        int chunkSize = chunkSizes[i];
        if (dataOffset + chunkSize > dataBuffer.length) {
          throw new IllegalStateException(
              "MiniBlockLayout chunk " + i + " extends past data buffer: offset="
                  + dataOffset + " size=" + chunkSize + " dataLen=" + dataBuffer.length);
        }
        byte[] chunkData = Arrays.copyOfRange(dataBuffer, dataOffset, dataOffset + chunkSize);
        dataOffset += chunkSize;

        // Parse chunk header
        int chunkOffset = 0;
        int numLevels = (chunkData[chunkOffset] & 0xFF) | ((chunkData[chunkOffset + 1] & 0xFF) << 8);
        chunkOffset += 2;

        // Read rep_size if present
        int repSize = 0;
        if (miniBlock.hasRepCompression()) {
          repSize = (chunkData[chunkOffset] & 0xFF) | ((chunkData[chunkOffset + 1] & 0xFF) << 8);
          chunkOffset += 2;
        }

        // Read def_size if present
        int defSize = 0;
        if (miniBlock.hasDefCompression() || hasNullableItem) {
          defSize = (chunkData[chunkOffset] & 0xFF) | ((chunkData[chunkOffset + 1] & 0xFF) << 8);
          chunkOffset += 2;
        }

        int[] bufferSizes = new int[numBuffers];
        for (int b = 0; b < numBuffers; b++) {
          if (hasLargeChunk) {
            bufferSizes[b] = (chunkData[chunkOffset] & 0xFF)
                | ((chunkData[chunkOffset + 1] & 0xFF) << 8)
                | ((chunkData[chunkOffset + 2] & 0xFF) << 16)
                | ((chunkData[chunkOffset + 3] & 0xFF) << 24);
            chunkOffset += 4;
          } else {
            bufferSizes[b] = (chunkData[chunkOffset] & 0xFF) | ((chunkData[chunkOffset + 1] & 0xFF) << 8);
            chunkOffset += 2;
          }
        }

        // Pad header to 8-byte alignment
        chunkOffset += (8 - (chunkOffset % 8)) % 8;

        // Skip rep buffer
        if (repSize > 0) {
          chunkOffset += repSize;
          chunkOffset += (8 - (chunkOffset % 8)) % 8;
        }

        // Extract and decode def buffer
        if (defSize > 0) {
          byte[] defBytes = Arrays.copyOfRange(chunkData, chunkOffset, chunkOffset + defSize);
          chunkOffset += defSize;
          chunkOffset += (8 - (chunkOffset % 8)) % 8;

          if (!hasListLayer || hasNullableItem) {
            ByteBuffer defBuf;
            if (miniBlock.hasDefCompression()) {
              PageBufferStore defStore = new PageBufferStore(List.of(defBytes));
              defBuf = CompressiveEncodingDecoders.decode(
                  miniBlock.getDefCompression(), numLevels, defStore, allocator);
            } else {
              defBuf = ByteBuffer.wrap(defBytes).order(ByteOrder.LITTLE_ENDIAN);
            }

            short[] defLevels = new short[numLevels];
            defBuf.asShortBuffer().get(defLevels);
            boolean[] chunkValidity = new boolean[numLevels];
            if (hasListLayer) {
              int nullItemLevel = computeNullItemLevel(layers);
              int maxVisibleLevel = computeMaxVisibleLevel(layers);
              // Filter out masked entries (def > maxVisibleLevel) so validity aligns with data values.
              int visibleCount = 0;
              for (int j = 0; j < numLevels; j++) {
                if (defLevels[j] <= maxVisibleLevel) {
                  visibleCount++;
                }
              }
              chunkValidity = new boolean[visibleCount];
              int vidx = 0;
              for (int j = 0; j < numLevels; j++) {
                if (defLevels[j] <= maxVisibleLevel) {
                  chunkValidity[vidx] = (defLevels[j] != nullItemLevel);
                  vidx++;
                }
              }
            } else {
              for (int j = 0; j < numLevels; j++) {
                chunkValidity[j] = (defLevels[j] == 0);
              }
            }
            chunkValidities.add(chunkValidity);
          }
        }

        // Extract all buffers
        List<byte[]> buffers = new ArrayList<>();
        for (int b = 0; b < numBuffers; b++) {
          if (chunkOffset + bufferSizes[b] > chunkData.length) {
            throw new IllegalStateException(
                "MiniBlockLayout chunk " + i + " buffer " + b + " extends past chunk: offset="
                    + chunkOffset + " size=" + bufferSizes[b] + " chunkLen=" + chunkData.length);
          }
          byte[] buf = Arrays.copyOfRange(chunkData, chunkOffset, chunkOffset + bufferSizes[b]);
          buffers.add(buf);
          chunkOffset += bufferSizes[b];
          chunkOffset += (8 - (chunkOffset % 8)) % 8;
        }

        // Calculate number of values in this chunk
        long chunkNumValues;
        if (logNumValues[i] == 0) {
          chunkNumValues = miniBlock.getNumItems() - valsInPrevChunks;
        } else {
          chunkNumValues = 1L << logNumValues[i];
        }
        valsInPrevChunks += chunkNumValues;

        // Decode the value buffers
        PageBufferStore chunkStore;
        if (miniBlock.getValueCompression().getCompressionCase()
            == lance.encodings21.EncodingsV21.CompressiveEncoding.CompressionCase.RLE) {
          // RLE in MiniBlockLayout stores values and run_lengths as separate buffers.
          // Merge them into the format expected by decodeRle: header + values + lengths.
          byte[] valuesBuf = buffers.get(0);
          byte[] lengthsBuf = buffers.get(1);
          byte[] merged = new byte[8 + valuesBuf.length + lengthsBuf.length];
          ByteBuffer.wrap(merged).order(ByteOrder.LITTLE_ENDIAN).putLong(valuesBuf.length);
          System.arraycopy(valuesBuf, 0, merged, 8, valuesBuf.length);
          System.arraycopy(lengthsBuf, 0, merged, 8 + valuesBuf.length, lengthsBuf.length);
          chunkStore = new PageBufferStore(List.of(merged));
        } else {
          chunkStore = new PageBufferStore(buffers);
        }
        if (isVariableWidth) {
          FieldVector decodedVec = CompressiveEncodingDecoders.decodeToVector(
              miniBlock.getValueCompression(), (int) chunkNumValues, chunkStore, field, allocator);
          miniBlockVectors.add(decodedVec);
        } else {
          ByteBuffer decoded = CompressiveEncodingDecoders.decode(
              miniBlock.getValueCompression(), (int) chunkNumValues, chunkStore, allocator);
          miniBlockData.add(decoded);
          totalBytes += decoded.remaining();
        }
      }
    } else {
      // Legacy path: each remaining buffer is a standalone mini-block
      int numMiniBlocks;
      if (hasNullableItem && miniBlock.hasDefCompression()) {
        int buffersPerBlock = (int) numBuffersPerMiniBlock + 1;
        numMiniBlocks = remainingBuffers / buffersPerBlock;
        if (numMiniBlocks == 0) {
          throw new IllegalStateException(
              "MiniBlockLayout: no mini-block buffers available (total="
                  + store.getBufferCount() + ", consumed=" + store.getCurrentBufferIndex() + ")");
        }
        for (int i = 0; i < numMiniBlocks; i++) {
          byte[] defBytes = store.takeNextBuffer();
          ByteBuffer defData;
          if (miniBlock.hasDefCompression()) {
            PageBufferStore defStore = new PageBufferStore(List.of(defBytes));
            defData = CompressiveEncodingDecoders.decode(
                miniBlock.getDefCompression(), numRows, defStore, allocator);
          } else {
            defData = ByteBuffer.wrap(defBytes).order(ByteOrder.LITTLE_ENDIAN);
          }
          boolean[] chunkValidity = new boolean[numRows];
          for (int j = 0; j < numRows; j++) {
            int byteIdx = j / 8;
            int bitIdx = j % 8;
            if (byteIdx < defData.remaining()) {
              chunkValidity[j] = (defData.get(byteIdx) & (1 << bitIdx)) != 0;
            }
          }
          chunkValidities.add(chunkValidity);

          if (isVariableWidth) {
            FieldVector decodedVec = CompressiveEncodingDecoders.decodeToVector(
                miniBlock.getValueCompression(), numRows, store, field, allocator);
            miniBlockVectors.add(decodedVec);
          } else {
            ByteBuffer mb = CompressiveEncodingDecoders.decode(
                miniBlock.getValueCompression(), 0, store, allocator);
            miniBlockData.add(mb);
            totalBytes += mb.remaining();
          }
        }
      } else {
        numMiniBlocks = remainingBuffers / (int) numBuffersPerMiniBlock;
        if (numMiniBlocks == 0) {
          throw new IllegalStateException(
              "MiniBlockLayout: no mini-block buffers available (total="
                  + store.getBufferCount() + ", consumed=" + store.getCurrentBufferIndex() + ")");
        }
        for (int i = 0; i < numMiniBlocks; i++) {
          if (isVariableWidth) {
            FieldVector decodedVec = CompressiveEncodingDecoders.decodeToVector(
                miniBlock.getValueCompression(), numRows, store, field, allocator);
            miniBlockVectors.add(decodedVec);
          } else {
            ByteBuffer mb = CompressiveEncodingDecoders.decode(
                miniBlock.getValueCompression(), 0, store, allocator);
            miniBlockData.add(mb);
            totalBytes += mb.remaining();
          }
        }
      }
    }

    boolean[] validity = null;
    if (!chunkValidities.isEmpty()) {
      int totalValid = chunkValidities.stream().mapToInt(a -> a.length).sum();
      validity = new boolean[totalValid];
      int vpos = 0;
      for (boolean[] cv : chunkValidities) {
        System.arraycopy(cv, 0, validity, vpos, cv.length);
        vpos += cv.length;
      }
    }

    int vectorSize = hasListLayer ? (int) miniBlock.getNumItems() : numRows;

    if (isVariableWidth) {
      FieldVector result = mergeVariableWidthVectors(miniBlockVectors, vectorSize, validity, field, allocator);
      for (FieldVector v : miniBlockVectors) {
        v.close();
      }
      return result;
    }

    byte[] allData = new byte[totalBytes];
    int pos = 0;
    for (ByteBuffer bb : miniBlockData) {
      int len = bb.remaining();
      bb.get(allData, pos, len);
      pos += len;
    }

    ByteBuffer allBuf = ByteBuffer.wrap(allData).order(ByteOrder.LITTLE_ENDIAN);

    if (hasDictionary) {
      int numDictItems = (int) miniBlock.getNumDictionaryItems();
      FieldVector dictValues = CompressiveEncodingDecoders.decodeToVector(
          miniBlock.getDictionary(), numDictItems, store, field, allocator);
      FieldVector result = expandDictionary(
          dictValues, allBuf, bitsPerValue, vectorSize, validity, field, allocator);
      dictValues.close();
      return result;
    }

    // V2.1 packed struct: value compression is PackedStruct and field is Struct.
    if (field.getType() instanceof org.apache.arrow.vector.types.pojo.ArrowType.Struct
        && miniBlock.getValueCompression().getCompressionCase()
            == lance.encodings21.EncodingsV21.CompressiveEncoding.CompressionCase.PACKED_STRUCT) {
      return CompressiveEncodingDecoders.decodePackedStructToVector(
          miniBlock.getValueCompression().getPackedStruct(), vectorSize, allBuf, field, allocator);
    }

    if (validity != null) {
      // The value buffer contains all values (including placeholders for nulls)
      // stored at physical width.  Extract only the valid values into a tightly
      // packed buffer for buildWithValidity.
      int physicalBytesPerValue = bitsPerValue / 8;
      org.apache.arrow.vector.FieldVector tempVec = field.createVector(allocator);
      int logicalBytesPerValue = FixedWidthVectorBuilder.getLogicalBytesPerValue(tempVec);
      tempVec.close();

      int validCount = 0;
      for (boolean v : validity) {
        if (v) {
          validCount++;
        }
      }
      byte[] validData = new byte[validCount * logicalBytesPerValue];
      int dstPos = 0;
      int srcPos = 0;
      for (int i = 0; i < vectorSize; i++) {
        if (validity[i]) {
          if (newFormat) {
            allBuf.position(i * physicalBytesPerValue);
          } else {
            allBuf.position(srcPos);
            srcPos += physicalBytesPerValue;
          }
          allBuf.get(validData, dstPos, logicalBytesPerValue);
          dstPos += logicalBytesPerValue;
        }
      }
      ByteBuffer validBuf = ByteBuffer.wrap(validData).order(ByteOrder.LITTLE_ENDIAN);
      return FixedWidthVectorBuilder.buildWithValidity(
          field, vectorSize, validBuf, bitsPerValue, validity, allocator);
    }

    return FixedWidthVectorBuilder.build(field, vectorSize, allBuf, bitsPerValue, allocator);
  }

  private static FieldVector expandDictionary(
      FieldVector dictValues, ByteBuffer indicesBuf, int indexBitsPerValue,
      int numRows, boolean[] validity, Field field, BufferAllocator allocator) {
    FieldVector result = field.createVector(allocator);
    result.allocateNew();

    FieldVector indicesVec = FixedWidthVectorBuilder.build(
        CompressiveEncodingDecoders.createIndexField(indexBitsPerValue),
        numRows, indicesBuf, indexBitsPerValue, allocator);

    org.apache.arrow.vector.util.TransferPair transferPair = dictValues.makeTransferPair(result);

    for (int i = 0; i < numRows; i++) {
      if (validity != null && !validity[i]) {
        result.setNull(i);
      } else {
        int idx = CompressiveEncodingDecoders.readIndex(indicesVec, i);
        transferPair.copyValueSafe(idx, i);
      }
    }

    result.setValueCount(numRows);
    indicesVec.close();
    return result;
  }

  private static FieldVector mergeVariableWidthVectors(
      List<FieldVector> chunks, int vectorSize, boolean[] validity,
      Field field, BufferAllocator allocator) {
    FieldVector result = field.createVector(allocator);
    result.allocateNew();

    int destIndex = 0;
    for (FieldVector src : chunks) {
      int srcCount = src.getValueCount();
      for (int i = 0; i < srcCount && destIndex < vectorSize; i++) {
        if (src.isNull(i)) {
          result.setNull(destIndex);
        } else {
          result.copyFromSafe(i, destIndex, src);
        }
        destIndex++;
      }
    }

    // Apply any explicit validity array
    if (validity != null) {
      for (int i = 0; i < Math.min(validity.length, vectorSize); i++) {
        if (!validity[i]) {
          result.setNull(i);
        }
      }
    }

    result.setValueCount(vectorSize);
    return result;
  }

  private static int readMetaWord(byte[] buffer, int index, int wordSize) {
    if (wordSize == 2) {
      return (buffer[index] & 0xFF) | ((buffer[index + 1] & 0xFF) << 8);
    } else {
      return (buffer[index] & 0xFF)
          | ((buffer[index + 1] & 0xFF) << 8)
          | ((buffer[index + 2] & 0xFF) << 16)
          | ((buffer[index + 3] & 0xFF) << 24);
    }
  }

  /**
   * Extracts raw definition levels from a MiniBlockLayout page.
   *
   * <p>This is used by {@link com.github.jlance.format.LanceFileReader} to obtain
   * structural nullability information (e.g. V2.1 nullable struct layers) that is
   * not exposed by the decoded vector alone.
   *
   * @return a {@code short[]} containing one definition level per item, or {@code null}
   *         if the layout has no definition levels or uses an unsupported format
   */
  public static short[] extractDefinitionLevels(
      PageLayout layout, int numRows, PageBufferStore store, BufferAllocator allocator) {
    var miniBlock = layout.getMiniBlockLayout();
    List<RepDefLayer> layers = miniBlock.getLayersList();
    boolean hasDefLevels = miniBlock.hasDefCompression()
        || layers.stream().anyMatch(l ->
            l == RepDefLayer.REPDEF_NULLABLE_ITEM
                || l == RepDefLayer.REPDEF_NULLABLE_LIST
                || l == RepDefLayer.REPDEF_EMPTYABLE_LIST
                || l == RepDefLayer.REPDEF_NULL_AND_EMPTY_LIST);
    if (!hasDefLevels) {
      return null;
    }

    boolean hasLargeChunk = miniBlock.getHasLargeChunk();
    int metaWordSize = hasLargeChunk ? 4 : 2;
    long numBuffersPerMiniBlock = miniBlock.getNumBuffers();
    int numBuffers = (int) numBuffersPerMiniBlock;
    int remainingBuffers = store.getBufferCount() - store.getCurrentBufferIndex();

    boolean newFormat = false;
    int[] chunkSizes = null;
    int[] logNumValues = null;
    int numChunks = 0;
    byte[] metaBuffer = null;
    byte[] dataBuffer = null;

    // Try Case A: separate buffers
    if (remainingBuffers >= 2) {
      byte[] candidateMeta = store.getBuffer(store.getCurrentBufferIndex());
      byte[] candidateData = store.getBuffer(store.getCurrentBufferIndex() + 1);
      if (candidateMeta.length % metaWordSize == 0) {
        int candidateChunks = candidateMeta.length / metaWordSize;
        int totalChunkSize = 0;
        for (int i = 0; i < candidateChunks; i++) {
          int word = readMetaWord(candidateMeta, i * metaWordSize, metaWordSize);
          int dividedBytes = word >>> 4;
          totalChunkSize += (dividedBytes + 1) * 8;
        }
        if (totalChunkSize == candidateData.length) {
          if (candidateData.length >= 2) {
            int numLevels = (candidateData[0] & 0xFF) | ((candidateData[1] & 0xFF) << 8);
            if (numLevels >= 0 && numLevels <= Math.max(numRows, 1024)) {
              newFormat = true;
            }
          }
          if (newFormat) {
            numChunks = candidateChunks;
            chunkSizes = new int[numChunks];
            logNumValues = new int[numChunks];
            for (int i = 0; i < numChunks; i++) {
              int word = readMetaWord(candidateMeta, i * metaWordSize, metaWordSize);
              logNumValues[i] = word & 0x0F;
              int dividedBytes = word >>> 4;
              chunkSizes[i] = (dividedBytes + 1) * 8;
            }
            metaBuffer = candidateMeta;
            dataBuffer = candidateData;
          }
        }
      }
    }

    // Try Case B: single buffer
    if (!newFormat && remainingBuffers >= 1) {
      byte[] firstBuffer = store.getBuffer(store.getCurrentBufferIndex());
      if (firstBuffer.length >= metaWordSize) {
        int offset = 0;
        int totalChunkSize = 0;
        int candidateChunks = 0;
        while (offset + metaWordSize <= firstBuffer.length) {
          int word = readMetaWord(firstBuffer, offset, metaWordSize);
          int dividedBytes = word >>> 4;
          int chunkSize = (dividedBytes + 1) * 8;
          if (offset + metaWordSize + chunkSize > firstBuffer.length) {
            break;
          }
          offset += metaWordSize;
          totalChunkSize += chunkSize;
          candidateChunks++;
          if (candidateChunks > 4096) {
            break;
          }
        }
        if (candidateChunks > 0
            && metaWordSize * candidateChunks + totalChunkSize <= firstBuffer.length) {
          newFormat = true;
          numChunks = candidateChunks;
          chunkSizes = new int[numChunks];
          logNumValues = new int[numChunks];
          offset = 0;
          for (int i = 0; i < numChunks; i++) {
            int word = readMetaWord(firstBuffer, offset, metaWordSize);
            logNumValues[i] = word & 0x0F;
            chunkSizes[i] = ((word >>> 4) + 1) * 8;
            offset += metaWordSize;
          }
          int totalChunkSize2 = 0;
          for (int s : chunkSizes) {
            totalChunkSize2 += s;
          }
          metaBuffer = Arrays.copyOfRange(firstBuffer, 0, metaWordSize * numChunks);
          dataBuffer = Arrays.copyOfRange(
              firstBuffer, metaWordSize * numChunks, metaWordSize * numChunks + totalChunkSize2);
        }
      }
    }

    if (!newFormat) {
      // Legacy path not supported for definition-level extraction
      return null;
    }

    List<Short> allDefLevels = new ArrayList<>();
    int dataOffset = 0;

    for (int i = 0; i < numChunks; i++) {
      int chunkSize = chunkSizes[i];
      if (dataOffset + chunkSize > dataBuffer.length) {
        return null;
      }
      byte[] chunkData = Arrays.copyOfRange(dataBuffer, dataOffset, dataOffset + chunkSize);
      dataOffset += chunkSize;

      int chunkOffset = 0;
      int numLevels = (chunkData[chunkOffset] & 0xFF) | ((chunkData[chunkOffset + 1] & 0xFF) << 8);
      chunkOffset += 2;

      int repSize = 0;
      if (miniBlock.hasRepCompression()) {
        repSize = (chunkData[chunkOffset] & 0xFF) | ((chunkData[chunkOffset + 1] & 0xFF) << 8);
        chunkOffset += 2;
      }

      int defSize = 0;
      if (miniBlock.hasDefCompression() || hasDefLevels) {
        defSize = (chunkData[chunkOffset] & 0xFF) | ((chunkData[chunkOffset + 1] & 0xFF) << 8);
        chunkOffset += 2;
      }

      int[] bufferSizes = new int[numBuffers];
      for (int b = 0; b < numBuffers; b++) {
        if (hasLargeChunk) {
          bufferSizes[b] = (chunkData[chunkOffset] & 0xFF)
              | ((chunkData[chunkOffset + 1] & 0xFF) << 8)
              | ((chunkData[chunkOffset + 2] & 0xFF) << 16)
              | ((chunkData[chunkOffset + 3] & 0xFF) << 24);
          chunkOffset += 4;
        } else {
          bufferSizes[b] = (chunkData[chunkOffset] & 0xFF)
              | ((chunkData[chunkOffset + 1] & 0xFF) << 8);
          chunkOffset += 2;
        }
      }

      chunkOffset += (8 - (chunkOffset % 8)) % 8;

      if (repSize > 0) {
        chunkOffset += repSize;
        chunkOffset += (8 - (chunkOffset % 8)) % 8;
      }

      if (defSize > 0) {
        byte[] defBytes = Arrays.copyOfRange(chunkData, chunkOffset, chunkOffset + defSize);
        chunkOffset += defSize;
        chunkOffset += (8 - (chunkOffset % 8)) % 8;

        ByteBuffer defBuf;
        if (miniBlock.hasDefCompression()) {
          PageBufferStore defStore = new PageBufferStore(List.of(defBytes));
          defBuf = CompressiveEncodingDecoders.decode(
              miniBlock.getDefCompression(), numLevels, defStore, allocator);
        } else {
          defBuf = ByteBuffer.wrap(defBytes).order(ByteOrder.LITTLE_ENDIAN);
        }

        if (defBuf.remaining() == numLevels) {
          for (int j = 0; j < numLevels; j++) {
            allDefLevels.add((short) (defBuf.get() & 0xFF));
          }
        } else if (defBuf.remaining() == numLevels * 2) {
          short[] defLevels = new short[numLevels];
          defBuf.asShortBuffer().get(defLevels);
          for (short d : defLevels) {
            allDefLevels.add(d);
          }
        } else {
          throw new UnsupportedOperationException(
              "Def buffer size " + defBuf.remaining() + " does not match expected numLevels " + numLevels);
        }
      }
    }

    short[] result = new short[allDefLevels.size()];
    for (int i = 0; i < allDefLevels.size(); i++) {
      result[i] = allDefLevels.get(i);
    }
    return result;
  }

  /**
   * Extracts raw repetition levels from a MiniBlockLayout page.
   *
   * <p>This is used by {@link com.github.jlance.format.LanceFileReader} to obtain
   * list structural information (list boundaries) from V2.1+ list columns.
   *
   * @return a {@code short[]} containing one repetition level per item, or {@code null}
   *         if the layout has no repetition levels or uses an unsupported format
   */
  public static short[] extractRepetitionLevels(
      PageLayout layout, int numRows, PageBufferStore store, BufferAllocator allocator) {
    var miniBlock = layout.getMiniBlockLayout();
    if (!miniBlock.hasRepCompression()) {
      return null;
    }


    boolean hasLargeChunk = miniBlock.getHasLargeChunk();
    int metaWordSize = hasLargeChunk ? 4 : 2;
    long numBuffersPerMiniBlock = miniBlock.getNumBuffers();
    int numBuffers = (int) numBuffersPerMiniBlock;
    int remainingBuffers = store.getBufferCount() - store.getCurrentBufferIndex();

    boolean newFormat = false;
    int[] chunkSizes = null;
    int[] logNumValues = null;
    int numChunks = 0;
    byte[] metaBuffer = null;
    byte[] dataBuffer = null;

    // Try Case A: separate buffers
    if (remainingBuffers >= 2) {
      byte[] candidateMeta = store.getBuffer(store.getCurrentBufferIndex());
      byte[] candidateData = store.getBuffer(store.getCurrentBufferIndex() + 1);
      if (candidateMeta.length % metaWordSize == 0) {
        int candidateChunks = candidateMeta.length / metaWordSize;
        int totalChunkSize = 0;
        for (int i = 0; i < candidateChunks; i++) {
          int word = readMetaWord(candidateMeta, i * metaWordSize, metaWordSize);
          int dividedBytes = word >>> 4;
          totalChunkSize += (dividedBytes + 1) * 8;
        }
        if (totalChunkSize == candidateData.length) {
          if (candidateData.length >= 2) {
            int numLevels = (candidateData[0] & 0xFF) | ((candidateData[1] & 0xFF) << 8);
            if (numLevels >= 0 && numLevels <= Math.max(numRows, 1024)) {
              newFormat = true;
            }
          }
          if (newFormat) {
            numChunks = candidateChunks;
            chunkSizes = new int[numChunks];
            logNumValues = new int[numChunks];
            for (int i = 0; i < numChunks; i++) {
              int word = readMetaWord(candidateMeta, i * metaWordSize, metaWordSize);
              logNumValues[i] = word & 0x0F;
              int dividedBytes = word >>> 4;
              chunkSizes[i] = (dividedBytes + 1) * 8;
            }
            metaBuffer = candidateMeta;
            dataBuffer = candidateData;
          }
        }
      }
    }

    // Try Case B: single buffer
    if (!newFormat && remainingBuffers >= 1) {
      byte[] firstBuffer = store.getBuffer(store.getCurrentBufferIndex());
      if (firstBuffer.length >= metaWordSize) {
        int offset = 0;
        int totalChunkSize = 0;
        int candidateChunks = 0;
        while (offset + metaWordSize <= firstBuffer.length) {
          int word = readMetaWord(firstBuffer, offset, metaWordSize);
          int dividedBytes = word >>> 4;
          int chunkSize = (dividedBytes + 1) * 8;
          if (offset + metaWordSize + chunkSize > firstBuffer.length) {
            break;
          }
          offset += metaWordSize;
          totalChunkSize += chunkSize;
          candidateChunks++;
          if (candidateChunks > 4096) {
            break;
          }
        }
        if (candidateChunks > 0
            && metaWordSize * candidateChunks + totalChunkSize <= firstBuffer.length) {
          newFormat = true;
          numChunks = candidateChunks;
          chunkSizes = new int[numChunks];
          logNumValues = new int[numChunks];
          offset = 0;
          for (int i = 0; i < numChunks; i++) {
            int word = readMetaWord(firstBuffer, offset, metaWordSize);
            logNumValues[i] = word & 0x0F;
            chunkSizes[i] = ((word >>> 4) + 1) * 8;
            offset += metaWordSize;
          }
          int totalChunkSize2 = 0;
          for (int s : chunkSizes) {
            totalChunkSize2 += s;
          }
          metaBuffer = Arrays.copyOfRange(firstBuffer, 0, metaWordSize * numChunks);
          dataBuffer = Arrays.copyOfRange(
              firstBuffer, metaWordSize * numChunks, metaWordSize * numChunks + totalChunkSize2);
        }
      }
    }

    if (!newFormat) {
      return null;
    }

    List<Short> allRepLevels = new ArrayList<>();
    int dataOffset = 0;

    for (int i = 0; i < numChunks; i++) {
      int chunkSize = chunkSizes[i];
      if (dataOffset + chunkSize > dataBuffer.length) {
        return null;
      }
      byte[] chunkData = Arrays.copyOfRange(dataBuffer, dataOffset, dataOffset + chunkSize);
      dataOffset += chunkSize;

      int chunkOffset = 0;
      int numLevels = (chunkData[chunkOffset] & 0xFF) | ((chunkData[chunkOffset + 1] & 0xFF) << 8);
      chunkOffset += 2;

      int repSize = 0;
      if (miniBlock.hasRepCompression()) {
        repSize = (chunkData[chunkOffset] & 0xFF) | ((chunkData[chunkOffset + 1] & 0xFF) << 8);
        chunkOffset += 2;
      }

      int defSize = 0;
      boolean hasNullableItem = miniBlock.getLayersList().stream()
          .anyMatch(l -> l == RepDefLayer.REPDEF_NULLABLE_ITEM);
      if (miniBlock.hasDefCompression() || hasNullableItem) {
        defSize = (chunkData[chunkOffset] & 0xFF) | ((chunkData[chunkOffset + 1] & 0xFF) << 8);
        chunkOffset += 2;
      }

      int[] bufferSizes = new int[numBuffers];
      for (int b = 0; b < numBuffers; b++) {
        if (hasLargeChunk) {
          bufferSizes[b] = (chunkData[chunkOffset] & 0xFF)
              | ((chunkData[chunkOffset + 1] & 0xFF) << 8)
              | ((chunkData[chunkOffset + 2] & 0xFF) << 16)
              | ((chunkData[chunkOffset + 3] & 0xFF) << 24);
          chunkOffset += 4;
        } else {
          bufferSizes[b] = (chunkData[chunkOffset] & 0xFF)
              | ((chunkData[chunkOffset + 1] & 0xFF) << 8);
          chunkOffset += 2;
        }
      }

      chunkOffset += (8 - (chunkOffset % 8)) % 8;

      if (repSize > 0) {
        byte[] repBytes = Arrays.copyOfRange(chunkData, chunkOffset, chunkOffset + repSize);
        chunkOffset += repSize;
        chunkOffset += (8 - (chunkOffset % 8)) % 8;

        ByteBuffer repBuf;
        if (miniBlock.hasRepCompression()) {
          PageBufferStore repStore = new PageBufferStore(List.of(repBytes));
          repBuf = CompressiveEncodingDecoders.decode(
              miniBlock.getRepCompression(), numLevels, repStore, allocator);
        } else {
          repBuf = ByteBuffer.wrap(repBytes).order(ByteOrder.LITTLE_ENDIAN);
        }

        if (repBuf.remaining() == numLevels) {
          // Byte values (e.g. 8-bit unpacked bitpacking)
          for (int j = 0; j < numLevels; j++) {
            allRepLevels.add((short) (repBuf.get() & 0xFF));
          }
        } else if (repBuf.remaining() == numLevels * 2) {
          // Short values (e.g. 16-bit flat)
          short[] repLevels = new short[numLevels];
          repBuf.asShortBuffer().get(repLevels);
          for (short r : repLevels) {
            allRepLevels.add(r);
          }
        } else {
          throw new UnsupportedOperationException(
              "Rep buffer size " + repBuf.remaining() + " does not match expected numLevels " + numLevels);
        }
      }
    }

    short[] result = new short[allRepLevels.size()];
    for (int i = 0; i < allRepLevels.size(); i++) {
      result[i] = allRepLevels.get(i);
    }
    return result;
  }

  private static boolean isSupportedLayer(List<RepDefLayer> layers) {
    if (layers.isEmpty()) {
      return true;
    }
    for (RepDefLayer layer : layers) {
      switch (layer) {
        case REPDEF_ALL_VALID_ITEM:
        case REPDEF_NULLABLE_ITEM:
        case REPDEF_ALL_VALID_LIST:
        case REPDEF_NULLABLE_LIST:
        case REPDEF_EMPTYABLE_LIST:
        case REPDEF_NULL_AND_EMPTY_LIST:
          continue;
        default:
          return false;
      }
    }
    return true;
  }

  private static boolean hasListLayer(List<RepDefLayer> layers) {
    return layers.stream().anyMatch(l ->
        l == RepDefLayer.REPDEF_ALL_VALID_LIST
            || l == RepDefLayer.REPDEF_NULLABLE_LIST
            || l == RepDefLayer.REPDEF_EMPTYABLE_LIST
            || l == RepDefLayer.REPDEF_NULL_AND_EMPTY_LIST);
  }

  private static int computeNullItemLevel(List<RepDefLayer> layers) {
    int currentDef = 0;
    for (RepDefLayer layer : layers) {
      switch (layer) {
        case REPDEF_ALL_VALID_ITEM:
        case REPDEF_ALL_VALID_LIST:
          break;
        case REPDEF_NULLABLE_ITEM:
          return currentDef + 1;
        case REPDEF_NULLABLE_LIST:
        case REPDEF_EMPTYABLE_LIST:
          currentDef += 1;
          break;
        case REPDEF_NULL_AND_EMPTY_LIST:
          currentDef += 2;
          break;
        default:
          break;
      }
    }
    return -1;
  }

  private static int computeMaxVisibleLevel(List<RepDefLayer> layers) {
    int level = 0;
    for (RepDefLayer layer : layers) {
      if (layer == RepDefLayer.REPDEF_ALL_VALID_LIST
          || layer == RepDefLayer.REPDEF_NULLABLE_LIST
          || layer == RepDefLayer.REPDEF_EMPTYABLE_LIST
          || layer == RepDefLayer.REPDEF_NULL_AND_EMPTY_LIST) {
        break;
      }
      switch (layer) {
        case REPDEF_ALL_VALID_ITEM:
        case REPDEF_ALL_VALID_LIST:
          break;
        case REPDEF_NULLABLE_ITEM:
        case REPDEF_NULLABLE_LIST:
        case REPDEF_EMPTYABLE_LIST:
          level += 1;
          break;
        case REPDEF_NULL_AND_EMPTY_LIST:
          level += 2;
          break;
        default:
          break;
      }
    }
    return level;
  }
}

