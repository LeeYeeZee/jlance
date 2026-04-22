// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.format.decoder;

import com.github.jlance.format.buffer.BufferReader;
import com.github.jlance.format.buffer.PageBufferStore;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import lance.encodings.EncodingsV20.ArrayEncoding;
import lance.encodings21.EncodingsV21.PageLayout;
import lance.file.v2.File2.ColumnMetadata;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decodes a Lance page into an Arrow {@link FieldVector}.
 *
 * <p>Supports both V2.0 ({@link ArrayEncoding}) and V2.1+ ({@link PageLayout}) encoding schemes.
 * The appropriate decoding path is selected based on the file version.
 */
public class PageDecoder {
  private static final Logger LOG = LoggerFactory.getLogger(PageDecoder.class);

  private final FileChannel channel;
  private final BufferAllocator allocator;

  public PageDecoder(FileChannel channel, BufferAllocator allocator) {
    this.channel = channel;
    this.allocator = allocator;
  }

  /**
   * Decodes a single page into a vector (V2.0 path, for backwards compatibility).
   *
   * @param page the page info with buffer offsets
   * @param field the Arrow field to decode into
   * @param numRows number of rows in the page
   * @return the decoded vector
   */
  public FieldVector decodePage(
      ColumnMetadata.Page page, Field field, int numRows) throws IOException {
    return decodePage(page, field, numRows, false);
  }

  /**
   * Decodes a single page into a vector, selecting the V2.0 or V2.1+ path.
   *
   * @param page the page info with buffer offsets
   * @param field the Arrow field to decode into
   * @param numRows number of rows in the page
   * @param isV21 true to use the V2.1+ {@link PageLayout} decoding path
   * @return the decoded vector
   */
  /**
   * Decodes a single page into a {@link DecodedArray} with rep/def state (V2.1+ only).
   */
  public DecodedArray decodePageWithRepDef(
      ColumnMetadata.Page page, Field field, int numRows, boolean isV21) throws IOException {
    // Read all buffers for this page into a store
    List<byte[]> buffers = new ArrayList<>();
    for (int i = 0; i < page.getBufferOffsetsCount(); i++) {
      long offset = page.getBufferOffsets(i);
      long size = page.getBufferSizes(i);
      ByteBuffer buf = BufferReader.readBuffer(channel, offset, size);
      byte[] data = new byte[(int) size];
      buf.get(data);
      buffers.add(data);
    }
    PageBufferStore store = new PageBufferStore(buffers);

    if (isV21) {
      PageLayout layout = unpackPageLayout(page.getEncoding());
      if (layout == null) {
        throw new IllegalArgumentException("Page encoding is not a direct PageLayout (V2.1+)");
      }
      PageLayoutDecoder decoder = createPageLayoutDecoder(layout);
      return decoder.decodeWithRepDef(layout, numRows, store, field, allocator);
    } else {
      ArrayEncoding encoding = unpackArrayEncoding(page.getEncoding());
      if (encoding == null) {
        throw new IllegalArgumentException("Page encoding is not a direct ArrayEncoding (V2.0)");
      }
      ArrayDecoder decoder = createDecoder(encoding);
      FieldVector vector = decoder.decode(encoding, numRows, store, field, allocator);
      return new DecodedArray(vector);
    }
  }

  /**
   * Decodes a single page into a vector, selecting the V2.0 or V2.1+ path.
   *
   * @param page the page info with buffer offsets
   * @param field the Arrow field to decode into
   * @param numRows number of rows in the page
   * @param isV21 true to use the V2.1+ {@link PageLayout} decoding path
   * @return the decoded vector
   */
  public FieldVector decodePage(
      ColumnMetadata.Page page, Field field, int numRows, boolean isV21) throws IOException {
    return decodePageWithRepDef(page, field, numRows, isV21).vector;
  }

  /** Unpacks an ArrayEncoding from a page/column Encoding wrapper. */
  public static ArrayEncoding unpackArrayEncoding(lance.file.v2.File2.Encoding enc) {
    if (!enc.hasDirect()) return null;
    try {
      com.google.protobuf.Any any =
          com.google.protobuf.Any.parseFrom(enc.getDirect().getEncoding());
      return any.unpack(ArrayEncoding.class);
    } catch (Exception e) {
      return null;
    }
  }

  /** Unpacks a PageLayout from a page/column Encoding wrapper. */
  public static PageLayout unpackPageLayout(lance.file.v2.File2.Encoding enc) {
    if (!enc.hasDirect()) return null;
    try {
      com.google.protobuf.Any any =
          com.google.protobuf.Any.parseFrom(enc.getDirect().getEncoding());
      return any.unpack(PageLayout.class);
    } catch (Exception e) {
      return null;
    }
  }

  /** Recursively creates an ArrayDecoder tree matching the ArrayEncoding tree. */
  public static ArrayDecoder createDecoder(ArrayEncoding encoding) {
    if (encoding.hasFlat()) {
      return new FlatDecoder();
    }
    if (encoding.hasNullable()) {
      var nullable = encoding.getNullable();
      if (nullable.hasNoNulls()) {
        return new NullableDecoder(createDecoder(nullable.getNoNulls().getValues()));
      }
      if (nullable.hasAllNulls()) {
        return new NullableDecoder(null); // No inner decoder needed for all nulls
      }
      if (nullable.hasSomeNulls()) {
        return new NullableDecoder(createDecoder(nullable.getSomeNulls().getValues()));
      }
      throw new IllegalArgumentException("Unknown nullability in Nullable encoding");
    }
    if (encoding.hasStruct()) {
      // SimpleStruct has no embedded child encodings; struct assembly happens at the file level.
      throw new UnsupportedOperationException(
          "SimpleStruct should be decoded at file level, not page level");
    }
    if (encoding.hasBinary()) {
      return new BinaryDecoder();
    }
    if (encoding.hasFixedSizeList()) {
      var fsl = encoding.getFixedSizeList();
      return new FixedSizeListDecoder(createDecoder(fsl.getItems()));
    }
    if (encoding.hasDictionary()) {
      return new DictionaryDecoder();
    }
    if (encoding.hasConstant()) {
      return new ConstantDecoder();
    }
    if (encoding.hasFixedSizeBinary()) {
      // Lance FixedSizeBinary encoding wraps an inner bytes encoding.
      // For now, fall back to FlatDecoder if the underlying data is flat bytes.
      var fsb = encoding.getFixedSizeBinary();
      return createDecoder(fsb.getBytes());
    }
    if (encoding.hasBitpacked()) {
      return new BitpackedDecoder();
    }
    if (encoding.hasRle()) {
      return new RleDecoder();
    }
    if (encoding.hasPackedStruct()) {
      return new PackedStructDecoder();
    }
    if (encoding.hasList()) {
      // List encoding is handled at the file level in LanceFileReader.decodeListColumn.
      throw new UnsupportedOperationException(
          "List encoding should be decoded at file level, not page level");
    }
    throw new UnsupportedOperationException(
        "Unsupported encoding type: " + encoding.getArrayEncodingCase());
  }

  /** Creates a PageLayoutDecoder for the given V2.1+ page layout. */
  public PageLayoutDecoder createPageLayoutDecoder(PageLayout layout) {
    if (layout.hasConstantLayout()) {
      return new ConstantLayoutDecoder();
    }
    if (layout.hasFullZipLayout()) {
      return new FullZipLayoutDecoder();
    }
    if (layout.hasMiniBlockLayout()) {
      return new MiniBlockLayoutDecoder();
    }
    if (layout.hasBlobLayout()) {
      return new BlobLayoutDecoder(channel);
    }
    throw new UnsupportedOperationException(
        "Unsupported page layout type: " + layout.getLayoutCase());
  }
}
