// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.compat;

import static org.junit.jupiter.api.Assertions.*;

import com.github.jlance.format.LanceFileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

/**
 * Compatibility tests for Milestone 31: V2.1 Int64.
 */
public class Milestone31CompatTest {

  private static final Path DATA_DIR = Paths.get("..", "compat_tests", "data", "milestone_31");

  @Test
  public void testInt64() throws Exception {
    Path file = DATA_DIR.resolve("test_int64.lance");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(5000, root.getRowCount());

        BigIntVector int64Col = (BigIntVector) root.getVector("int64_col");
        assertEquals(5000, int64Col.getValueCount());
        // Boundary values at start
        assertEquals(Long.MAX_VALUE, int64Col.get(0));
        assertEquals(Long.MIN_VALUE, int64Col.get(1));
        assertEquals(0L, int64Col.get(2));
        assertEquals(-1L, int64Col.get(3));
        assertEquals(1L, int64Col.get(4));

        BigIntVector nullableVec = (BigIntVector) root.getVector("nullable_int64");
        assertEquals(5000, nullableVec.getValueCount());
        // Spot-check nulls (20% null rate, exact positions depend on seed)
        // Just verify some rows are null and some are not
        int nullCount = 0;
        int nonNullCount = 0;
        for (int i = 0; i < 5000; i++) {
          if (nullableVec.isNull(i)) {
            nullCount++;
          } else {
            nonNullCount++;
          }
        }
        assertTrue(nullCount > 500 && nullCount < 1500, "Expected ~20% nulls, got " + nullCount);
        assertTrue(nonNullCount > 3500, "Expected ~80% non-nulls, got " + nonNullCount);

        BigIntVector largeVec = (BigIntVector) root.getVector("large_int64");
        assertEquals(5000, largeVec.getValueCount());
      }
    }
  }
}
