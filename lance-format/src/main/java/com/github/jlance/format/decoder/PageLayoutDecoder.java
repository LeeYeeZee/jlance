package com.github.jlance.format.decoder;

import com.github.jlance.format.buffer.PageBufferStore;
import lance.encodings21.EncodingsV21.PageLayout;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Decodes a Lance V2.1+ {@link PageLayout} into an Arrow {@link FieldVector}.
 *
 * <p>V2.1 separates structural layout (how data is organized in pages) from
 * compressive encoding (how values are compressed). A {@code PageLayoutDecoder}
 * handles the structural aspect, delegating value decompression to a
 * {@link CompressiveEncodingDecoder} tree.
 */
public interface PageLayoutDecoder {

  /**
   * Decodes the given page layout into a {@link DecodedArray} containing both the
   * vector and the rep/def state.
   *
   * @param layout the page layout for this page
   * @param numRows number of rows to decode
   * @param store buffer store containing all page buffers
   * @param field the Arrow field descriptor
   * @param allocator memory allocator for Arrow vectors
   * @return the decoded array with rep/def state
   */
  DecodedArray decodeWithRepDef(
      PageLayout layout,
      int numRows,
      PageBufferStore store,
      Field field,
      BufferAllocator allocator);

  /**
   * Decodes the given page layout into a vector.
   *
   * <p>Default implementation delegates to {@link #decodeWithRepDef} and returns
   * the vector only.
   *
   * @param layout the page layout for this page
   * @param numRows number of rows to decode
   * @param store buffer store containing all page buffers
   * @param field the Arrow field descriptor
   * @param allocator memory allocator for Arrow vectors
   * @return the decoded vector
   */
  default FieldVector decode(
      PageLayout layout,
      int numRows,
      PageBufferStore store,
      Field field,
      BufferAllocator allocator) {
    return decodeWithRepDef(layout, numRows, store, field, allocator).vector;
  }
}
