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
 * Compatibility tests for Milestone 33: V2.1 ConstantLayout (all-same / all-null).
 */
public class Milestone33CompatTest {

  private static final Path DATA_DIR = Paths.get("..", "compat_tests", "data", "milestone_33");

  @Test
  public void testConstant() throws Exception {
    Path file = DATA_DIR.resolve("test_constant.lance");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(10000, root.getRowCount());

        BigIntVector sameInt64 = (BigIntVector) root.getVector("same_int64");
        assertEquals(10000, sameInt64.getValueCount());
        for (int i = 0; i < 10000; i++) {
          assertEquals(42L, sameInt64.get(i), "same_int64 mismatch at " + i);
        }

        Float8Vector sameFloat64 = (Float8Vector) root.getVector("same_float64");
        assertEquals(10000, sameFloat64.getValueCount());
        for (int i = 0; i < 10000; i++) {
          assertEquals(3.14159, sameFloat64.get(i), 1e-9, "same_float64 mismatch at " + i);
        }

        BigIntVector allNullInt64 = (BigIntVector) root.getVector("all_null_int64");
        assertEquals(10000, allNullInt64.getValueCount());
        for (int i = 0; i < 10000; i++) {
          assertTrue(allNullInt64.isNull(i), "all_null_int64 should be null at " + i);
        }

        Float4Vector allNullFloat32 = (Float4Vector) root.getVector("all_null_float32");
        assertEquals(10000, allNullFloat32.getValueCount());
        for (int i = 0; i < 10000; i++) {
          assertTrue(allNullFloat32.isNull(i), "all_null_float32 should be null at " + i);
        }

        IntVector sameInt32 = (IntVector) root.getVector("same_int32");
        assertEquals(10000, sameInt32.getValueCount());
        for (int i = 0; i < 10000; i++) {
          assertEquals(99, sameInt32.get(i), "same_int32 mismatch at " + i);
        }

        IntVector mixedNormal = (IntVector) root.getVector("mixed_normal");
        assertEquals(10000, mixedNormal.getValueCount());
        for (int i = 0; i < 10000; i++) {
          assertEquals(i, mixedNormal.get(i), "mixed_normal mismatch at " + i);
        }
      }
    }
  }
}
