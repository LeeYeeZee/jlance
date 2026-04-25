// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.compat;

import static org.junit.jupiter.api.Assertions.*;

import com.github.jlance.format.LanceFileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.junit.jupiter.api.Test;

/**
 * Compatibility test for nullable struct with mixed child nullability.
 * Verifies that a struct is considered null only when the struct itself
 * is null, not when individual children are null.
 */
public class Milestone62CompatTest {

  private static final Path DATA_DIR = Paths.get("..", "compat_tests", "data", "milestone_62");

  @Test
  public void testNullableStructMixedNulls() throws Exception {
    Path file = DATA_DIR.resolve("test_nullable_struct_mixed_nulls.lance");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(5, root.getRowCount());

        StructVector structVec = (StructVector) root.getVector("s");

        // Row 0: struct present, a=1, b="x"
        assertFalse(structVec.isNull(0));
        IntVector aVec = (IntVector) structVec.getChild("a");
        VarCharVector bVec = (VarCharVector) structVec.getChild("b");
        assertEquals(1, aVec.get(0));
        assertEquals("x", bVec.getObject(0).toString());

        // Row 1: struct present, a=null, b="y"
        assertFalse(structVec.isNull(1));
        assertTrue(aVec.isNull(1));
        assertEquals("y", bVec.getObject(1).toString());

        // Row 2: struct present, a=2, b=null
        assertFalse(structVec.isNull(2));
        assertEquals(2, aVec.get(2));
        assertTrue(bVec.isNull(2));

        // Row 3: struct null
        assertTrue(structVec.isNull(3));
        // When struct is null, children are null at that row
        assertTrue(aVec.isNull(3));
        assertTrue(bVec.isNull(3));

        // Row 4: struct present, a=null, b=null
        assertFalse(structVec.isNull(4));
        assertTrue(aVec.isNull(4));
        assertTrue(bVec.isNull(4));
      }
    }
  }
}
