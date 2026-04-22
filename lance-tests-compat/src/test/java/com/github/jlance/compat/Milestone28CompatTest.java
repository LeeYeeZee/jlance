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
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.junit.jupiter.api.Test;

/**
 * Compatibility tests for Milestone 28: Struct with null field.
 */
public class Milestone28CompatTest {

  private static Path dataPath(String name) {
    return Paths.get("..", "compat_tests", "data", "milestone_28", name);
  }

  @Test
  public void testStructWithNullField() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(dataPath("test_struct_null.lance"))) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(4, root.getRowCount());
        StructVector vec = (StructVector) root.getVector("s");
        IntVector aVec = (IntVector) vec.getChild("a");
        NullVector bVec = (NullVector) vec.getChild("b");

        assertEquals(1, aVec.get(0));
        assertTrue(bVec.isNull(0));

        assertEquals(2, aVec.get(1));
        assertTrue(bVec.isNull(1));

        assertTrue(aVec.isNull(2));
        assertTrue(bVec.isNull(2));

        assertEquals(4, aVec.get(3));
        assertTrue(bVec.isNull(3));
      }
    }
  }
}
