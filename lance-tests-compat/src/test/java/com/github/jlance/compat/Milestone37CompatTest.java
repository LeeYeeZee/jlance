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
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

/**
 * Compatibility tests for Milestone 37: V2.1 mixed primitives.
 */
public class Milestone37CompatTest {

  private static final Path DATA_DIR = Paths.get("..", "compat_tests", "data", "milestone_37");

  @Test
  public void testMixed() throws Exception {
    Path file = DATA_DIR.resolve("test_mixed.lance");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(1000, root.getRowCount());
        assertEquals(6, root.getSchema().getFields().size());

        BigIntVector int64Vec = (BigIntVector) root.getVector("int64_col");
        assertEquals(1000, int64Vec.getValueCount());

        IntVector int32Vec = (IntVector) root.getVector("int32_col");
        assertEquals(1000, int32Vec.getValueCount());

        Float4Vector float32Vec = (Float4Vector) root.getVector("float32_col");
        assertEquals(1000, float32Vec.getValueCount());

        Float8Vector float64Vec = (Float8Vector) root.getVector("float64_col");
        assertEquals(1000, float64Vec.getValueCount());

        BigIntVector nullableInt64 = (BigIntVector) root.getVector("nullable_int64");
        assertEquals(1000, nullableInt64.getValueCount());
        int nullCount = 0;
        for (int i = 0; i < 1000; i++) {
          if (nullableInt64.isNull(i)) nullCount++;
        }
        assertTrue(nullCount > 100 && nullCount < 300,
            "Expected ~20% nulls, got " + nullCount);

        Float8Vector nullableFloat64 = (Float8Vector) root.getVector("nullable_float64");
        assertEquals(1000, nullableFloat64.getValueCount());
        nullCount = 0;
        for (int i = 0; i < 1000; i++) {
          if (nullableFloat64.isNull(i)) nullCount++;
        }
        assertTrue(nullCount > 200 && nullCount < 400,
            "Expected ~30% nulls, got " + nullCount);
      }
    }
  }
}
