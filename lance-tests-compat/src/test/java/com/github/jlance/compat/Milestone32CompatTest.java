// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.compat;

import static org.junit.jupiter.api.Assertions.*;

import com.github.jlance.format.LanceFileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

/**
 * Compatibility tests for Milestone 32: V2.1 Float32/Float64.
 */
public class Milestone32CompatTest {

  private static final Path DATA_DIR = Paths.get("..", "compat_tests", "data", "milestone_32");

  @Test
  public void testFloat() throws Exception {
    Path file = DATA_DIR.resolve("test_float.lance");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(3000, root.getRowCount());

        Float4Vector f32Vec = (Float4Vector) root.getVector("float32_col");
        assertEquals(3000, f32Vec.getValueCount());
        assertEquals(0.0f, f32Vec.get(0), 1e-9f);
        assertEquals(-0.0f, f32Vec.get(1), 1e-9f);
        assertTrue(Float.isInfinite(f32Vec.get(2)) && f32Vec.get(2) > 0);
        assertTrue(Float.isInfinite(f32Vec.get(3)) && f32Vec.get(3) < 0);
        assertTrue(Float.isNaN(f32Vec.get(4)));

        Float8Vector f64Vec = (Float8Vector) root.getVector("float64_col");
        assertEquals(3000, f64Vec.getValueCount());
        assertEquals(0.0, f64Vec.get(0), 1e-9);
        assertEquals(Double.MAX_VALUE, f64Vec.get(1), 1e-9);
        assertEquals(-Double.MAX_VALUE, f64Vec.get(2), 1e-9);
        assertEquals(Double.MIN_NORMAL, f64Vec.get(3), 1e-9);
        assertTrue(Double.isInfinite(f64Vec.get(4)) && f64Vec.get(4) > 0);
        assertTrue(Double.isInfinite(f64Vec.get(5)) && f64Vec.get(5) < 0);

        Float4Vector nullableF32 = (Float4Vector) root.getVector("nullable_float32");
        assertEquals(3000, nullableF32.getValueCount());
        int nullCount = 0;
        for (int i = 0; i < 3000; i++) {
          if (nullableF32.isNull(i)) nullCount++;
        }
        assertTrue(nullCount > 500 && nullCount < 1000, "Expected ~25% nulls, got " + nullCount);

        Float8Vector nullableF64 = (Float8Vector) root.getVector("nullable_float64");
        assertEquals(3000, nullableF64.getValueCount());
      }
    }
  }
}
