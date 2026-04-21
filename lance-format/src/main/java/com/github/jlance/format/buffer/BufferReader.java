package com.github.jlance.format.buffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/** Utility to read a raw buffer from a file channel. */
public class BufferReader {

  public static ByteBuffer readBuffer(FileChannel channel, long offset, long size)
      throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate((int) size);
    channel.read(buffer, offset);
    buffer.flip();
    return buffer;
  }
}
