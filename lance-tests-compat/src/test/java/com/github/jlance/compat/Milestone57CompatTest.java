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
 * Compatibility test for 6-level nested list — list^6&lt;int32&gt;.
 */
public class Milestone57CompatTest {

  private static final Path DATA_DIR = Paths.get("..", "compat_tests", "data", "milestone_57");

  @Test
  public void testDeepNestedList() throws Exception {
    Path file = DATA_DIR.resolve("test_nested_list_6.lance");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(20, root.getRowCount());
        ListVector l6 = (ListVector) root.getVector("deep");
        assertNotNull(l6);
        ListVector l5 = (ListVector) l6.getDataVector();
        ListVector l4 = (ListVector) l5.getDataVector();
        ListVector l3 = (ListVector) l4.getDataVector();
        ListVector l2 = (ListVector) l3.getDataVector();
        ListVector l1 = (ListVector) l2.getDataVector();
        IntVector intVec = (IntVector) l1.getDataVector();

        // Row 0: null
        assertTrue(l6.isNull(0));

        // Row 1: empty
        assertFalse(l6.isNull(1));
        assertEquals(0, l6.getElementEndIndex(1) - l6.getElementStartIndex(1));

        // Row 2: [[[[[[]]]]]] => l6:1, l5:1, l4:1, l3:1, l2:1, l1:0
        assertFalse(l6.isNull(2));
        assertEquals(1, l6.getElementEndIndex(2) - l6.getElementStartIndex(2));
        int i5 = l5.getElementStartIndex(l6.getElementStartIndex(2));
        assertEquals(1, l5.getElementEndIndex(l6.getElementStartIndex(2)) - i5);
        int i4 = l4.getElementStartIndex(i5);
        assertEquals(1, l4.getElementEndIndex(i5) - i4);
        int i3 = l3.getElementStartIndex(i4);
        assertEquals(1, l3.getElementEndIndex(i4) - i3);
        int i2 = l2.getElementStartIndex(i3);
        assertEquals(1, l2.getElementEndIndex(i3) - i2);
        assertEquals(0, l1.getElementEndIndex(i2) - l1.getElementStartIndex(i2));

        // Row 3: [[[[[[1]]]]]] => l6:1, l5:1, l4:1, l3:1, l2:1, l1:1 (int: 1)
        assertFalse(l6.isNull(3));
        assertEquals(1, l6.getElementEndIndex(3) - l6.getElementStartIndex(3));
        i5 = l5.getElementStartIndex(l6.getElementStartIndex(3));
        i4 = l4.getElementStartIndex(i5);
        i3 = l3.getElementStartIndex(i4);
        i2 = l2.getElementStartIndex(i3);
        assertEquals(1, l1.getElementEndIndex(i2) - l1.getElementStartIndex(i2));
        assertEquals(1, intVec.get(l1.getElementStartIndex(i2)));

        // Row 4: [[[[[[1,2]], [[3,4,5]]]]]]
        // l6:1, l5:1, l4:1, l3:1 (with 2 l2 children)
        assertFalse(l6.isNull(4));
        assertEquals(1, l6.getElementEndIndex(4) - l6.getElementStartIndex(4));
        i5 = l5.getElementStartIndex(l6.getElementStartIndex(4));
        i4 = l4.getElementStartIndex(i5);
        i3 = l3.getElementStartIndex(i4);
        assertEquals(2, l3.getElementEndIndex(i4) - i3);
        int i2a = l2.getElementStartIndex(i3);
        assertEquals(1, l2.getElementEndIndex(i3) - i2a);
        int i1a = l1.getElementStartIndex(i2a);
        assertEquals(2, l1.getElementEndIndex(i2a) - i1a);
        assertEquals(1, intVec.get(i1a));
        assertEquals(2, intVec.get(i1a + 1));
        int i2b = l2.getElementStartIndex(i3 + 1);
        assertEquals(1, l2.getElementEndIndex(i3 + 1) - i2b);
        int i1b = l1.getElementStartIndex(i2b);
        assertEquals(3, l1.getElementEndIndex(i2b) - i1b);
        assertEquals(3, intVec.get(i1b));
        assertEquals(4, intVec.get(i1b + 1));
        assertEquals(5, intVec.get(i1b + 2));

        // Row 5: [[[[[[10]]]], [[[[20,30]]]]]]
        // l6:1, l5:1 (with 2 l4 children)
        assertFalse(l6.isNull(5));
        assertEquals(1, l6.getElementEndIndex(5) - l6.getElementStartIndex(5));
        i5 = l5.getElementStartIndex(l6.getElementStartIndex(5));
        assertEquals(2, l5.getElementEndIndex(l6.getElementStartIndex(5)) - i5);
        i4 = l4.getElementStartIndex(i5);
        assertEquals(1, l4.getElementEndIndex(i5) - i4);
        int i4a = i4;
        int i4b = i4 + 1;
        i3 = l3.getElementStartIndex(i4a);
        assertEquals(1, l3.getElementEndIndex(i4a) - i3);
        assertEquals(1, l1.getElementEndIndex(l2.getElementStartIndex(l3.getElementStartIndex(i4a))) - l1.getElementStartIndex(l2.getElementStartIndex(l3.getElementStartIndex(i4a))));
        i3 = l3.getElementStartIndex(i4b);
        assertEquals(1, l3.getElementEndIndex(i4b) - i3);
        assertEquals(2, l1.getElementEndIndex(l2.getElementStartIndex(i3)) - l1.getElementStartIndex(l2.getElementStartIndex(i3)));

        // Row 19: [[[[[[26,27,28,29,30]]]]]]
        assertFalse(l6.isNull(19));
        assertEquals(1, l6.getElementEndIndex(19) - l6.getElementStartIndex(19));
        i5 = l5.getElementStartIndex(l6.getElementStartIndex(19));
        i4 = l4.getElementStartIndex(i5);
        i3 = l3.getElementStartIndex(i4);
        i2 = l2.getElementStartIndex(i3);
        assertEquals(5, l1.getElementEndIndex(i2) - l1.getElementStartIndex(i2));
      }
    }
  }
}
