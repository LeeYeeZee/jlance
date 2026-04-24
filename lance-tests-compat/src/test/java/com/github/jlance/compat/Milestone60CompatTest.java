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
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.junit.jupiter.api.Test;

/**
 * Compatibility test for nullable struct&lt;nullable int&gt; — stacked NullableItem.
 */
public class Milestone60CompatTest {

  private static final Path DATA_DIR = Paths.get("..", "compat_tests", "data", "milestone_60");

  @Test
  public void testNullableStructNullableInt() throws Exception {
    Path file = DATA_DIR.resolve("test_nullable_struct_nullable_int.lance");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(5, root.getRowCount());

        StructVector sVec = (StructVector) root.getVector("s");
        assertNotNull(sVec);
        IntVector xVec = (IntVector) sVec.getChild("x");
        assertNotNull(xVec);

        // Row 0: {x: 1} — valid struct, valid int
        assertFalse(sVec.isNull(0));
        assertFalse(xVec.isNull(0));
        assertEquals(1, xVec.get(0));

        // Row 1: {x: null} — valid struct, null int
        assertFalse(sVec.isNull(1));
        assertTrue(xVec.isNull(1));

        // Row 2: null — null struct
        assertTrue(sVec.isNull(2));

        // Row 3: {x: 10} — valid struct, valid int
        assertFalse(sVec.isNull(3));
        assertFalse(xVec.isNull(3));
        assertEquals(10, xVec.get(3));

        // Row 4: null — null struct
        assertTrue(sVec.isNull(4));
      }
    }
  }
}
