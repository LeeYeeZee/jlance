// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.format.decoder;

import com.github.jlance.format.buffer.PageBufferStore;
import lance.encodings.EncodingsV20.ArrayEncoding;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Field;

/** Decodes an {@link ArrayEncoding} tree into an Arrow {@link FieldVector}. */
public interface ArrayDecoder {
  /**
   * Decodes the given encoding into a vector.
   *
   * @param encoding the root encoding for this column/page
   * @param numRows number of rows to decode
   * @param store buffer store containing all page buffers
   * @param field the Arrow field descriptor
   * @param allocator memory allocator for Arrow vectors
   * @return the decoded vector
   */
  FieldVector decode(
      ArrayEncoding encoding,
      int numRows,
      PageBufferStore store,
      Field field,
      BufferAllocator allocator);
}
