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
import org.apache.arrow.vector.complex.ListVector;
import org.junit.jupiter.api.Test;

/**
 * Test for mixed-layout (MiniBlock + ConstantLayout) multi-page list&lt;int&gt;.
 */
public class MixedLayoutListTest {

  private static final Path DATA_DIR = Paths.get("..", "compat_tests", "data", "milestone_61");

  @Test
  public void testMixedLayoutList() throws Exception {
    Path file = DATA_DIR.resolve("test_mixed_layout_list.lance");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(10, root.getRowCount());

        ListVector vec = (ListVector) root.getVector("items");
        assertNotNull(vec);
        assertEquals(10, vec.getValueCount());

        IntVector data = (IntVector) vec.getDataVector();

        // Row 0: [1, 2, 3]
        assertFalse(vec.isNull(0));
        assertEquals(0, vec.getElementStartIndex(0));
        assertEquals(3, vec.getElementEndIndex(0));
        assertEquals(1, data.get(0));
        assertEquals(2, data.get(1));
        assertEquals(3, data.get(2));

        // Row 1: []
        assertFalse(vec.isNull(1));
        assertEquals(3, vec.getElementStartIndex(1));
        assertEquals(3, vec.getElementEndIndex(1));

        // Row 2: null
        assertTrue(vec.isNull(2));

        // Row 3: [4, 5]
        assertFalse(vec.isNull(3));
        assertEquals(3, vec.getElementStartIndex(3));
        assertEquals(5, vec.getElementEndIndex(3));
        assertEquals(4, data.get(3));
        assertEquals(5, data.get(4));

        // Row 4: []
        assertFalse(vec.isNull(4));
        assertEquals(5, vec.getElementStartIndex(4));
        assertEquals(5, vec.getElementEndIndex(4));

        // Row 5: [6]
        assertFalse(vec.isNull(5));
        assertEquals(5, vec.getElementStartIndex(5));
        assertEquals(6, vec.getElementEndIndex(5));
        assertEquals(6, data.get(5));

        // Row 6: null
        assertTrue(vec.isNull(6));

        // Row 7: [7, 8, 9, 10]
        assertFalse(vec.isNull(7));
        assertEquals(6, vec.getElementStartIndex(7));
        assertEquals(10, vec.getElementEndIndex(7));
        assertEquals(7, data.get(6));
        assertEquals(8, data.get(7));
        assertEquals(9, data.get(8));
        assertEquals(10, data.get(9));

        // Row 8: []
        assertFalse(vec.isNull(8));
        assertEquals(10, vec.getElementStartIndex(8));
        assertEquals(10, vec.getElementEndIndex(8));

        // Row 9: [11]
        assertFalse(vec.isNull(9));
        assertEquals(10, vec.getElementStartIndex(9));
        assertEquals(11, vec.getElementEndIndex(9));
        assertEquals(11, data.get(10));
      }
    }
  }
}
