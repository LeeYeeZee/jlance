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
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

/**
 * Compatibility tests for Milestone 34: V2.1 multi-page numeric.
 */
public class Milestone34CompatTest {

  private static final Path DATA_DIR = Paths.get("..", "compat_tests", "data", "milestone_34");

  @Test
  public void testMultiPage() throws Exception {
    Path file = DATA_DIR.resolve("test_multi_page.lance");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(50000, root.getRowCount());

        BigIntVector multiInt64 = (BigIntVector) root.getVector("multi_int64");
        assertEquals(50000, multiInt64.getValueCount());

        Float8Vector multiFloat64 = (Float8Vector) root.getVector("multi_float64");
        assertEquals(50000, multiFloat64.getValueCount());

        BigIntVector nullableVec = (BigIntVector) root.getVector("multi_nullable_int64");
        assertEquals(50000, nullableVec.getValueCount());
        int nullCount = 0;
        for (int i = 0; i < 50000; i++) {
          if (nullableVec.isNull(i)) nullCount++;
        }
        assertTrue(nullCount > 10000 && nullCount < 20000,
            "Expected ~30% nulls, got " + nullCount);
      }
    }
  }
}
