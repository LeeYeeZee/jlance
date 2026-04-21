package com.github.jlance.compat;

import static org.junit.jupiter.api.Assertions.*;

import com.github.jlance.format.LanceFileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.junit.jupiter.api.Test;

/**
 * Compatibility test for deeply nested lists (3+ layers).
 *
 * <p>These tests verify that the recursive V21ListUnraveler correctly handles
 * arbitrary nesting depth, as required by the generalized nested list decoder.
 */
public class Milestone44CompatTest {

  private static final Path DATA_DIR = Paths.get("..", "compat_tests", "data", "milestone_44");

  private static Path dataPath(String name) {
    return DATA_DIR.resolve(name + ".lance");
  }

  @Test
  public void testNested3Layer() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(dataPath("test_nested_3layer"))) {
      var metadata = reader.readMetadata();
      assertEquals(4, metadata.getNumRows());

      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(1, root.getFieldVectors().size());
        ListVector outer = (ListVector) root.getVector("val");
        assertEquals(4, outer.getValueCount());

        // Row 0: [[[1,2], [3]]]  -> outer has 1 list
        assertFalse(outer.isNull(0));
        int o0 = outer.getElementStartIndex(0);
        int o1 = outer.getElementEndIndex(0);
        assertEquals(1, o1 - o0, "Row 0 outer should have 1 inner list");

        ListVector middle = (ListVector) outer.getDataVector();
        // The single middle list in row 0 has 2 inner lists
        assertEquals(2, middle.getElementEndIndex(o0) - middle.getElementStartIndex(o0),
            "Row 0 middle should have 2 lists");

        ListVector inner = (ListVector) middle.getDataVector();
        IntVector items = (IntVector) inner.getDataVector();

        // First inner list: [1, 2]
        int m0 = middle.getElementStartIndex(o0);
        int i0 = inner.getElementStartIndex(m0);
        assertEquals(1, items.get(i0));
        assertEquals(2, items.get(i0 + 1));

        // Second inner list: [3]
        int m1 = m0 + 1;
        int i1 = inner.getElementStartIndex(m1);
        assertEquals(3, items.get(i1));

        // Row 1: null
        assertTrue(outer.isNull(1));

        // Row 2: [[[]]] -> outer=1, middle=1, inner=0 items
        assertFalse(outer.isNull(2));
        int o2 = outer.getElementStartIndex(2);
        assertEquals(1, outer.getElementEndIndex(2) - o2);
        int m2 = middle.getElementStartIndex(o2);
        assertEquals(1, middle.getElementEndIndex(o2) - m2);
        assertEquals(0, inner.getElementEndIndex(m2) - inner.getElementStartIndex(m2));

        // Row 3: [[[4]], [[5,6], [7]]] -> outer=2 lists
        assertFalse(outer.isNull(3));
        int o3 = outer.getElementStartIndex(3);
        assertEquals(2, outer.getElementEndIndex(3) - o3);

        // First outer list: [[4]] -> middle=1, inner=1 item
        int m3_0 = middle.getElementStartIndex(o3);
        assertEquals(1, middle.getElementEndIndex(o3) - m3_0);
        int i3_0 = inner.getElementStartIndex(m3_0);
        assertEquals(1, inner.getElementEndIndex(m3_0) - i3_0);
        assertEquals(4, items.get(i3_0));

        // Second outer list: [[5,6], [7]] -> middle=2, items=3
        int m3_1 = m3_0 + 1;
        assertEquals(2, middle.getElementEndIndex(o3 + 1) - middle.getElementStartIndex(o3 + 1));
        int i3_1 = inner.getElementStartIndex(m3_1);
        assertEquals(5, items.get(i3_1));
        assertEquals(6, items.get(i3_1 + 1));
        int i3_2 = inner.getElementStartIndex(m3_1 + 1);
        assertEquals(7, items.get(i3_2));
      }
    }
  }

  @Test
  public void testNested3LayerAllNull() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(dataPath("test_nested_3layer_all_null"))) {
      var metadata = reader.readMetadata();
      assertEquals(3, metadata.getNumRows());

      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        ListVector outer = (ListVector) root.getVector("val");
        assertEquals(3, outer.getValueCount());
        assertTrue(outer.isNull(0));
        assertTrue(outer.isNull(1));
        assertTrue(outer.isNull(2));
      }
    }
  }

  @Test
  public void testNested3LayerEmptyMiddle() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(dataPath("test_nested_3layer_empty_middle"))) {
      var metadata = reader.readMetadata();
      assertEquals(3, metadata.getNumRows());

      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        ListVector outer = (ListVector) root.getVector("val");
        assertEquals(3, outer.getValueCount());

        ListVector middle = (ListVector) outer.getDataVector();
        ListVector inner = (ListVector) middle.getDataVector();
        IntVector items = (IntVector) inner.getDataVector();

        // Row 0: [[[]], [[]]] -> outer=2, each middle=1, each inner=0
        assertFalse(outer.isNull(0));
        int o0 = outer.getElementStartIndex(0);
        assertEquals(2, outer.getElementEndIndex(0) - o0);

        for (int j = 0; j < 2; j++) {
          int mj = middle.getElementStartIndex(o0 + j);
          assertEquals(1, middle.getElementEndIndex(o0 + j) - mj,
              "Row 0 middle list " + j + " should have 1 inner list");
          assertEquals(0, inner.getElementEndIndex(mj) - inner.getElementStartIndex(mj),
              "Row 0 inner list " + j + " should be empty");
        }

        // Row 1: [[[]], [[1]]] -> outer=2, middle0=1 inner empty, middle1=1 inner with 1 item
        assertFalse(outer.isNull(1));
        int o1 = outer.getElementStartIndex(1);
        assertEquals(2, outer.getElementEndIndex(1) - o1);

        int m1_0 = middle.getElementStartIndex(o1);
        assertEquals(1, middle.getElementEndIndex(o1) - m1_0);
        assertEquals(0, inner.getElementEndIndex(m1_0) - inner.getElementStartIndex(m1_0),
            "Row 1 first inner list should be empty");

        int m1_1 = middle.getElementStartIndex(o1 + 1);
        assertEquals(1, middle.getElementEndIndex(o1 + 1) - m1_1);
        int i1_1 = inner.getElementStartIndex(m1_1);
        assertEquals(1, inner.getElementEndIndex(m1_1) - i1_1);
        assertEquals(1, items.get(i1_1));

        // Row 2: null
        assertTrue(outer.isNull(2));
      }
    }
  }

  @Test
  public void testNested4Layer() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(dataPath("test_nested_4layer"))) {
      var metadata = reader.readMetadata();
      assertEquals(3, metadata.getNumRows());

      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        ListVector l0 = (ListVector) root.getVector("val");
        assertEquals(3, l0.getValueCount());

        // Row 0: [[[[1,2]], [[3]]]] -> l0=1, l1=2, l2=1, l3=2, items=3
        assertFalse(l0.isNull(0));
        int i0 = l0.getElementStartIndex(0);
        assertEquals(1, l0.getElementEndIndex(0) - i0);

        ListVector l1 = (ListVector) l0.getDataVector();
        assertEquals(2, l1.getElementEndIndex(i0) - l1.getElementStartIndex(i0));

        ListVector l2 = (ListVector) l1.getDataVector();
        ListVector l3 = (ListVector) l2.getDataVector();
        IntVector items = (IntVector) l3.getDataVector();

        int j0 = l1.getElementStartIndex(i0);
        assertEquals(1, l2.getElementEndIndex(j0) - l2.getElementStartIndex(j0));
        int k0 = l2.getElementStartIndex(j0);
        assertEquals(2, l3.getElementEndIndex(k0) - l3.getElementStartIndex(k0));
        assertEquals(1, items.get(l3.getElementStartIndex(k0)));
        assertEquals(2, items.get(l3.getElementStartIndex(k0) + 1));

        int j1 = j0 + 1;
        assertEquals(1, l2.getElementEndIndex(j1) - l2.getElementStartIndex(j1));
        int k1 = l2.getElementStartIndex(j1);
        assertEquals(1, l3.getElementEndIndex(k1) - l3.getElementStartIndex(k1));
        assertEquals(3, items.get(l3.getElementStartIndex(k1)));

        // Row 1: null
        assertTrue(l0.isNull(1));

        // Row 2: [[[[4]]]] -> l0=1, l1=1, l2=1, l3=1, items=1
        assertFalse(l0.isNull(2));
        int i2 = l0.getElementStartIndex(2);
        assertEquals(1, l0.getElementEndIndex(2) - i2);

        int j2 = l1.getElementStartIndex(i2);
        assertEquals(1, l1.getElementEndIndex(i2) - j2);

        int k2 = l2.getElementStartIndex(j2);
        assertEquals(1, l2.getElementEndIndex(j2) - k2);

        int m2 = l3.getElementStartIndex(k2);
        assertEquals(1, l3.getElementEndIndex(k2) - m2);
        assertEquals(4, items.get(m2));
      }
    }
  }
}
