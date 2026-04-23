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
 * Compatibility test for 5-level nested list — list^5&lt;int32&gt;.
 */
public class Milestone56CompatTest {

  private static final Path DATA_DIR = Paths.get("..", "compat_tests", "data", "milestone_56");

  @Test
  public void testAllNull() throws Exception {
    Path file = DATA_DIR.resolve("test_nested_list_5.lance");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(20, root.getRowCount());
        ListVector vec = (ListVector) root.getVector("all_null");
        assertNotNull(vec);
        for (int i = 0; i < 20; i++) {
          assertTrue(vec.isNull(i), "Row " + i + " should be null");
        }
      }
    }
  }

  @Test
  public void testAllEmpty() throws Exception {
    Path file = DATA_DIR.resolve("test_nested_list_5.lance");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(20, root.getRowCount());
        ListVector vec = (ListVector) root.getVector("all_empty");
        assertNotNull(vec);
        for (int i = 0; i < 20; i++) {
          assertFalse(vec.isNull(i), "Row " + i + " should not be null");
          assertEquals(0, vec.getElementEndIndex(i) - vec.getElementStartIndex(i),
              "Row " + i + " should be empty");
        }
      }
    }
  }

  @Test
  public void testMixed() throws Exception {
    Path file = DATA_DIR.resolve("test_nested_list_5.lance");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(20, root.getRowCount());
        ListVector l5 = (ListVector) root.getVector("mixed");
        assertNotNull(l5);
        ListVector l4 = (ListVector) l5.getDataVector();
        ListVector l3 = (ListVector) l4.getDataVector();
        ListVector l2 = (ListVector) l3.getDataVector();
        ListVector l1 = (ListVector) l2.getDataVector();
        IntVector intVec = (IntVector) l1.getDataVector();

        // Row 0: null
        assertTrue(l5.isNull(0));

        // Row 1: empty outer
        assertFalse(l5.isNull(1));
        assertEquals(0, l5.getElementEndIndex(1) - l5.getElementStartIndex(1));

        // Row 2: [[[[[]]]]] => l5:1, l4:1, l3:1, l2:1, l1:0
        assertFalse(l5.isNull(2));
        assertEquals(1, l5.getElementEndIndex(2) - l5.getElementStartIndex(2));
        int i4 = l4.getElementStartIndex(l5.getElementStartIndex(2));
        assertEquals(1, l4.getElementEndIndex(l5.getElementStartIndex(2)) - i4);
        int i3 = l3.getElementStartIndex(i4);
        assertEquals(1, l3.getElementEndIndex(i4) - i3);
        int i2 = l2.getElementStartIndex(i3);
        assertEquals(1, l2.getElementEndIndex(i3) - i2);
        assertEquals(0, l1.getElementEndIndex(i2) - l1.getElementStartIndex(i2));

        // Row 3: [[[[[]], [[1,2,3]]]]] => l5:1, l4:1, l3:1 (with 2 l2 children)
        assertFalse(l5.isNull(3));
        assertEquals(1, l5.getElementEndIndex(3) - l5.getElementStartIndex(3));
        i4 = l4.getElementStartIndex(l5.getElementStartIndex(3));
        assertEquals(1, l4.getElementEndIndex(l5.getElementStartIndex(3)) - i4);
        i3 = l3.getElementStartIndex(i4);
        assertEquals(2, l3.getElementEndIndex(i4) - i3);  // l3 has 2 l2 children
        int i2a = l2.getElementStartIndex(i3);
        assertEquals(1, l2.getElementEndIndex(i3) - i2a);
        assertEquals(0, l1.getElementEndIndex(i2a) - l1.getElementStartIndex(i2a));
        int i2b = l2.getElementStartIndex(i3 + 1);
        assertEquals(1, l2.getElementEndIndex(i3 + 1) - i2b);
        int i1b = l1.getElementStartIndex(i2b);
        assertEquals(3, l1.getElementEndIndex(i2b) - i1b);
        assertEquals(1, intVec.get(i1b));
        assertEquals(2, intVec.get(i1b + 1));
        assertEquals(3, intVec.get(i1b + 2));

        // Row 4: [[[[[10,20]], [[30,40,50]]], [[[60]]]]]
        // l5:1, l4:2, first l4->l3:2, second l4->l3:1
        assertFalse(l5.isNull(4));
        assertEquals(1, l5.getElementEndIndex(4) - l5.getElementStartIndex(4));
        i4 = l4.getElementStartIndex(l5.getElementStartIndex(4));
        assertEquals(2, l4.getElementEndIndex(l5.getElementStartIndex(4)) - i4);
        int i3a = l3.getElementStartIndex(i4);
        assertEquals(2, l3.getElementEndIndex(i4) - i3a);
        int i3b = l3.getElementStartIndex(i4 + 1);
        assertEquals(1, l3.getElementEndIndex(i4 + 1) - i3b);

        // Row 5: [[[[[1]]]], [[[[2,3]], [[4,5,6]]]]]
        // l5:2, first l5->l4:1, second l5->l4:1 (with 2 l2 children)
        assertFalse(l5.isNull(5));
        assertEquals(2, l5.getElementEndIndex(5) - l5.getElementStartIndex(5));
        int i4a = l4.getElementStartIndex(l5.getElementStartIndex(5));
        assertEquals(1, l4.getElementEndIndex(l5.getElementStartIndex(5)) - i4a);
        int i4b = l4.getElementStartIndex(l5.getElementStartIndex(5) + 1);
        assertEquals(1, l4.getElementEndIndex(l5.getElementStartIndex(5) + 1) - i4b);
        i3 = l3.getElementStartIndex(i4b);
        assertEquals(2, l3.getElementEndIndex(i4b) - i3);  // l3 has 2 l2 children
        i2a = l2.getElementStartIndex(i3);
        assertEquals(1, l2.getElementEndIndex(i3) - i2a);
        i1b = l1.getElementStartIndex(i2a);
        assertEquals(2, l1.getElementEndIndex(i2a) - i1b);
        assertEquals(2, intVec.get(i1b));
        assertEquals(3, intVec.get(i1b + 1));
        i2b = l2.getElementStartIndex(i3 + 1);
        assertEquals(1, l2.getElementEndIndex(i3 + 1) - i2b);
        int i1c = l1.getElementStartIndex(i2b);
        assertEquals(3, l1.getElementEndIndex(i2b) - i1c);
        assertEquals(4, intVec.get(i1c));
        assertEquals(5, intVec.get(i1c + 1));
        assertEquals(6, intVec.get(i1c + 2));

        // Row 9: [[[[[7,8]]]]]
        assertFalse(l5.isNull(9));
        assertEquals(1, l5.getElementEndIndex(9) - l5.getElementStartIndex(9));
        i4 = l4.getElementStartIndex(l5.getElementStartIndex(9));
        assertEquals(1, l4.getElementEndIndex(l5.getElementStartIndex(9)) - i4);

        // Row 10: [[[[[9]]]], [[[[10,11]]]]]
        assertFalse(l5.isNull(10));
        assertEquals(2, l5.getElementEndIndex(10) - l5.getElementStartIndex(10));

        // Row 15: [[[[[13,14]]]], [[[[15]]]]]
        assertFalse(l5.isNull(15));
        assertEquals(2, l5.getElementEndIndex(15) - l5.getElementStartIndex(15));
      }
    }
  }
}
