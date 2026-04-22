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
 * Compatibility tests for Milestone 26: Dictionary with non-string values.
 */
public class Milestone26CompatTest {

  private static Path dataPath(String name) {
    return Paths.get("..", "compat_tests", "data", "milestone_26", name);
  }

  @Test
  public void testDictionaryInt64Values() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(dataPath("test_dict_int.lance"))) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(6, root.getRowCount());
        BigIntVector vec = (BigIntVector) root.getVector("dict_int");
        assertEquals(100L, vec.get(0));
        assertEquals(200L, vec.get(1));
        assertEquals(100L, vec.get(2));
        assertEquals(300L, vec.get(3));
        assertTrue(vec.isNull(4));
        assertEquals(200L, vec.get(5));
      }
    }
  }
}
