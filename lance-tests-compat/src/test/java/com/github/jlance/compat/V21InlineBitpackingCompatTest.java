// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.compat;

import static org.junit.jupiter.api.Assertions.*;

import com.github.jlance.format.LanceFileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

/**
 * Compatibility test for V2.1 InlineBitpacking by reading Python-generated Lance files.
 */
public class V21InlineBitpackingCompatTest {

  private static final Path DATA_DIR = Paths.get("..", "compat_tests", "data", "milestone_39");

  @Test
  public void testReadBitpackingFile() throws Exception {
    Path file = DATA_DIR.resolve("test_bitpacking.lance");
    if (!java.nio.file.Files.exists(file)) {
      org.junit.jupiter.api.Assumptions.assumeTrue(false, "Test data not generated: " + file);
    }

    try (BufferAllocator allocator = new RootAllocator();
         LanceFileReader reader = new LanceFileReader(file)) {
      var footer = reader.readFooter();
      System.out.println("File version: " + footer.getMajorVersion() + "." + footer.getMinorVersion());
      System.out.println("Num columns: " + footer.getColumnMetadatas().size());

      var metadata = reader.readMetadata();
      System.out.println("Schema: " + metadata.getSchema());
      System.out.println("Row count: " + metadata.getNumRows());

      // Debug: print buffer info for first page of all columns
      for (int c = 0; c < footer.getColumnMetadatas().size(); c++) {
        var col = footer.getColumnMetadatas().get(c);
        for (int p = 0; p < Math.min(1, col.getPagesCount()); p++) {
          var page = col.getPages(p);
          System.out.println("=== Column " + c + " Page " + p + " ===");
          System.out.println("  length: " + page.getLength());
          System.out.println("  bufferOffsetsCount: " + page.getBufferOffsetsCount());
          System.out.println("  bufferOffsets: " + page.getBufferOffsetsList());
          System.out.println("  bufferSizes: " + page.getBufferSizesList());
          for (int b = 0; b < page.getBufferOffsetsCount(); b++) {
            long off = page.getBufferOffsets(b);
            long sz = page.getBufferSizes(b);
            System.out.println("  Buffer " + b + ": offset=" + off + " size=" + sz);
            try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(file.toFile(), "r")) {
              raf.seek(off);
              int printLen = (int) Math.min(sz, 64);
              byte[] buf = new byte[printLen];
              raf.readFully(buf);
              StringBuilder sb = new StringBuilder();
              for (byte bb : buf) sb.append(String.format("%02x ", bb & 0xff));
              System.out.println("    bytes: " + sb);
            }
          }
          var layout = com.github.jlance.format.decoder.PageDecoder.unpackPageLayout(page.getEncoding());
          if (layout != null && layout.hasMiniBlockLayout()) {
            var mb = layout.getMiniBlockLayout();
            System.out.println("  MiniBlock: num_buffers=" + mb.getNumBuffers() + " num_items=" + mb.getNumItems());
            System.out.println("  layers: " + mb.getLayersList());
            System.out.println("  hasDefCompression: " + mb.hasDefCompression());
            var vc = mb.getValueCompression();
            System.out.println("  value_compression case: " + vc.getCompressionCase());
            if (vc.hasInlineBitpacking()) {
              var ib = vc.getInlineBitpacking();
              System.out.println("  inline_bitpacking uncompressed_bits=" + ib.getUncompressedBitsPerValue());
              System.out.println("  inline_bitpacking hasValues=" + ib.hasValues());
              if (ib.hasValues()) {
                System.out.println("  inline_bitpacking compression scheme=" + ib.getValues().getScheme());
              }
            }
          }
        }
      }

      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(3, root.getFieldVectors().size());

        FieldVector smallIntVec = root.getVector("small_int");
        assertInstanceOf(IntVector.class, smallIntVec);
        IntVector intVec = (IntVector) smallIntVec;
        assertEquals(2050, intVec.getValueCount());
        for (int i = 0; i < 10; i++) {
          System.out.println("small_int[" + i + "] = " + intVec.get(i));
        }

        FieldVector u8Vec = root.getVector("u8_col");
        assertInstanceOf(UInt1Vector.class, u8Vec);
        UInt1Vector uint1Vec = (UInt1Vector) u8Vec;
        assertEquals(2050, uint1Vec.getValueCount());
        for (int i = 0; i < 10; i++) {
          System.out.println("u8_col[" + i + "] = " + (uint1Vec.get(i) & 0xFF));
        }
      }
    }
  }
}
