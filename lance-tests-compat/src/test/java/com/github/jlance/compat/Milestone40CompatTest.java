// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.compat;

import static org.junit.jupiter.api.Assertions.*;

import com.github.jlance.format.LanceFileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

/**
 * Compatibility tests for Milestone 40: BlobLayout (V2.2 large binary).
 */
public class Milestone40CompatTest {

  private static final Path DATA_DIR = Paths.get("..", "compat_tests", "data", "milestone_40");

  @Test
  public void testBlobLargeBinary() throws Exception {
    Path file = DATA_DIR.resolve("test_blob_large_binary.lance");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(20, root.getRowCount());

        LargeVarBinaryVector vec = (LargeVarBinaryVector) root.getVector("large_binary");
        assertEquals(20, vec.getValueCount());

        // Row 0: empty value
        assertFalse(vec.isNull(0));
        assertEquals(0, vec.get(0).length);

        // Row 1: null value
        assertTrue(vec.isNull(1));

        // Verify remaining rows have non-empty data
        int nullCount = 0;
        int emptyCount = 0;
        long totalSize = 0;
        for (int i = 0; i < 20; i++) {
          if (vec.isNull(i)) {
            nullCount++;
          } else {
            byte[] data = vec.get(i);
            if (data.length == 0) {
              emptyCount++;
            } else {
              totalSize += data.length;
              // Size should be in expected range (50KB - 150KB)
              assertTrue(
                  data.length >= 50000 && data.length <= 150000,
                  "Row " + i + " size " + data.length + " out of range");
            }
          }
        }
        assertEquals(1, nullCount, "Expected exactly 1 null");
        assertEquals(1, emptyCount, "Expected exactly 1 empty value");
        assertTrue(totalSize > 0, "Expected non-zero total size");
      }
    }
  }
}
