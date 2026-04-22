// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.format.decoder;

import com.github.jlance.format.buffer.BufferReader;
import com.github.jlance.format.buffer.PageBufferStore;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.List;
import lance.encodings21.EncodingsV21.BlobLayout;
import lance.encodings21.EncodingsV21.PageLayout;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decodes a {@link BlobLayout} into an Arrow {@link FieldVector}.
 *
 * <p>BlobLayout stores large binary data out-of-line in the file. The page only contains
 * "descriptors" (position + size pairs) encoded by the inner layout. The actual blob data is
 * read from the file at the absolute offsets given by the descriptors.
 *
 * <p>Validity is smuggled into the descriptors:
 * <ul>
 *   <li>{@code position=0, size=0} — empty value (zero-length byte array)</li>
 *   <li>{@code position!=0, size=0} — null value</li>
 *   <li>{@code position!=0, size!=0} — normal value, data at file offset {@code position}</li>
 * </ul>
 */
public class BlobLayoutDecoder implements PageLayoutDecoder {
  private static final Logger LOG = LoggerFactory.getLogger(BlobLayoutDecoder.class);

  private final FileChannel channel;

  public BlobLayoutDecoder(FileChannel channel) {
    this.channel = channel;
  }

  @Override
  public DecodedArray decodeWithRepDef(
      PageLayout layout,
      int numRows,
      PageBufferStore store,
      Field field,
      BufferAllocator allocator) {
    BlobLayout blob = layout.getBlobLayout();
    PageLayout innerLayout = blob.getInnerLayout();

    // Decode inner layout into a FixedSizeBinary(16) vector.
    // Each row is 16 bytes: position (u64) + size (u64).
    Field descField = new Field(
        "desc", FieldType.nullable(new ArrowType.FixedSizeBinary(16)), null);
    PageLayoutDecoder innerDecoder = new MiniBlockLayoutDecoder();
    FieldVector descVec = innerDecoder.decode(innerLayout, numRows, store, descField, allocator);

    if (!(descVec instanceof FixedSizeBinaryVector)) {
      descVec.close();
      throw new IllegalStateException(
          "BlobLayout inner layout did not produce FixedSizeBinaryVector, got: "
              + descVec.getClass().getName());
    }

    FixedSizeBinaryVector fsbVec = (FixedSizeBinaryVector) descVec;

    // Extract positions and sizes
    long[] positions = new long[numRows];
    long[] sizes = new long[numRows];
    int byteWidth = fsbVec.getByteWidth();
    org.apache.arrow.memory.ArrowBuf dataBuf = fsbVec.getDataBuffer();
    for (int i = 0; i < numRows; i++) {
      byte[] row = new byte[byteWidth];
      dataBuf.getBytes((long) i * byteWidth, row, 0, byteWidth);
      ByteBuffer buf = ByteBuffer.wrap(row).order(ByteOrder.LITTLE_ENDIAN);
      positions[i] = buf.getLong();
      sizes[i] = buf.getLong();
    }
    fsbVec.close();

    // Build the result vector
    FieldVector vector = field.createVector(allocator);
    if (vector instanceof VarCharVector) {
      vector = buildVarCharVector((VarCharVector) vector, numRows, positions, sizes);
    } else if (vector instanceof VarBinaryVector) {
      vector = buildVarBinaryVector((VarBinaryVector) vector, numRows, positions, sizes);
    } else if (vector instanceof LargeVarCharVector) {
      vector = buildLargeVarCharVector((LargeVarCharVector) vector, numRows, positions, sizes);
    } else if (vector instanceof LargeVarBinaryVector) {
      vector = buildLargeVarBinaryVector((LargeVarBinaryVector) vector, numRows, positions, sizes);
    } else {
      vector.close();
      throw new UnsupportedOperationException(
          "BlobLayout does not yet support vector type: " + vector.getClass().getName());
    }
    return new DecodedArray(vector);
  }

  private VarCharVector buildVarCharVector(
      VarCharVector vec, int numRows, long[] positions, long[] sizes) {
    vec.allocateNew(numRows);
    for (int i = 0; i < numRows; i++) {
      long pos = positions[i];
      long size = sizes[i];
      if (size == 0) {
        if (pos == 0) {
          // Empty value
          vec.setSafe(i, new byte[0]);
        } else {
          // Null value
          vec.setNull(i);
        }
      } else {
        byte[] data = readBlobData(pos, size);
        vec.setSafe(i, data);
      }
    }
    vec.setValueCount(numRows);
    return vec;
  }

  private VarBinaryVector buildVarBinaryVector(
      VarBinaryVector vec, int numRows, long[] positions, long[] sizes) {
    vec.allocateNew(numRows);
    for (int i = 0; i < numRows; i++) {
      long pos = positions[i];
      long size = sizes[i];
      if (size == 0) {
        if (pos == 0) {
          vec.setSafe(i, new byte[0]);
        } else {
          vec.setNull(i);
        }
      } else {
        byte[] data = readBlobData(pos, size);
        vec.setSafe(i, data);
      }
    }
    vec.setValueCount(numRows);
    return vec;
  }

  private LargeVarCharVector buildLargeVarCharVector(
      LargeVarCharVector vec, int numRows, long[] positions, long[] sizes) {
    vec.allocateNew(numRows);
    for (int i = 0; i < numRows; i++) {
      long pos = positions[i];
      long size = sizes[i];
      if (size == 0) {
        if (pos == 0) {
          vec.setSafe(i, new byte[0]);
        } else {
          vec.setNull(i);
        }
      } else {
        byte[] data = readBlobData(pos, size);
        vec.setSafe(i, data);
      }
    }
    vec.setValueCount(numRows);
    return vec;
  }

  private LargeVarBinaryVector buildLargeVarBinaryVector(
      LargeVarBinaryVector vec, int numRows, long[] positions, long[] sizes) {
    vec.allocateNew(numRows);
    for (int i = 0; i < numRows; i++) {
      long pos = positions[i];
      long size = sizes[i];
      if (size == 0) {
        if (pos == 0) {
          vec.setSafe(i, new byte[0]);
        } else {
          vec.setNull(i);
        }
      } else {
        byte[] data = readBlobData(pos, size);
        vec.setSafe(i, data);
      }
    }
    vec.setValueCount(numRows);
    return vec;
  }

  private byte[] readBlobData(long position, long size) {
    if (size > Integer.MAX_VALUE) {
      throw new UnsupportedOperationException(
          "Blob size too large: " + size);
    }
    int sz = (int) size;
    byte[] data = new byte[sz];
    try {
      ByteBuffer buf = ByteBuffer.wrap(data);
      int read = channel.read(buf, position);
      if (read != sz) {
        throw new IOException(
            "BlobLayout short read: expected " + sz + " bytes at position " + position
                + ", got " + read);
      }
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to read blob data at position " + position + " size " + size, e);
    }
    return data;
  }
}
