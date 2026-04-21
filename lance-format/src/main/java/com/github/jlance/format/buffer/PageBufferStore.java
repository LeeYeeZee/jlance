package com.github.jlance.format.buffer;

import java.util.List;

/**
 * Stores all buffers for a single page, indexed by the buffer number specified in the ArrayEncoding
 * protobuf.
 */
public class PageBufferStore {
  private final List<byte[]> buffers;
  private int nextBufferIndex = 0;

  public PageBufferStore(List<byte[]> buffers) {
    this.buffers = buffers;
  }

  /** Returns the buffer at the given index. */
  public byte[] getBuffer(int index) {
    return buffers.get(index);
  }

  /** Returns the number of buffers. */
  public int getBufferCount() {
    return buffers.size();
  }

  /**
   * Returns the next buffer and advances the implicit buffer index.
   *
   * <p>Used for V2.1+ decoding where buffers are consumed in order rather than
   * referenced by explicit index.
   */
  public byte[] takeNextBuffer() {
    if (nextBufferIndex >= buffers.size()) {
      throw new IllegalStateException(
          "No more buffers available (requested index "
              + nextBufferIndex
              + ", total "
              + buffers.size()
              + ")");
    }
    return buffers.get(nextBufferIndex++);
  }

  /** Returns the current implicit buffer index without consuming. */
  public int getCurrentBufferIndex() {
    return nextBufferIndex;
  }

  /** Resets the implicit buffer index to 0. */
  public void resetBufferIndex() {
    nextBufferIndex = 0;
  }
}
