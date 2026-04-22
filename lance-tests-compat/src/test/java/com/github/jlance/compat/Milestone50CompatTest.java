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
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.junit.jupiter.api.Test;

public class Milestone50CompatTest {

  private static final Path DATA_DIR = Paths.get("..", "compat_tests", "data", "milestone_50");

  @Test
  public void testListStructList() throws Exception {
    Path file = DATA_DIR.resolve("test_list_struct_list.lance");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(5, root.getRowCount());

        ListVector outerList = (ListVector) root.getVector("nested");
        assertFalse(outerList.isNull(0));
        assertEquals(2, outerList.getElementEndIndex(0) - outerList.getElementStartIndex(0));
        assertFalse(outerList.isNull(1));
        assertEquals(1, outerList.getElementEndIndex(1) - outerList.getElementStartIndex(1));
        assertTrue(outerList.isNull(2));
        assertFalse(outerList.isNull(3));
        assertEquals(0, outerList.getElementEndIndex(3) - outerList.getElementStartIndex(3));
        assertFalse(outerList.isNull(4));
        assertEquals(1, outerList.getElementEndIndex(4) - outerList.getElementStartIndex(4));

        StructVector structVec = (StructVector) outerList.getDataVector();
        ListVector innerList = (ListVector) structVec.getChild("items");
        VarCharVector nameVec = (VarCharVector) structVec.getChild("name");

        // Struct 0 (row 0, first): {"items": [1, 2], "name": "a"}
        assertEquals(2, innerList.getElementEndIndex(0) - innerList.getElementStartIndex(0));
        IntVector innerData = (IntVector) innerList.getDataVector();
        assertEquals(1, innerData.get(0));
        assertEquals(2, innerData.get(1));
        assertEquals("a", nameVec.getObject(0).toString());

        // Struct 1 (row 0, second): {"items": [3], "name": "b"}
        assertEquals(1, innerList.getElementEndIndex(1) - innerList.getElementStartIndex(1));
        assertEquals(3, innerData.get(2));
        assertEquals("b", nameVec.getObject(1).toString());

        // Struct 2 (row 1): {"items": [], "name": "c"}
        assertEquals(0, innerList.getElementEndIndex(2) - innerList.getElementStartIndex(2));
        assertEquals("c", nameVec.getObject(2).toString());

        // Struct 3 (row 4): {"items": [4, 5, 6], "name": "d"}
        assertEquals(3, innerList.getElementEndIndex(3) - innerList.getElementStartIndex(3));
        assertEquals(4, innerData.get(3));
        assertEquals(5, innerData.get(4));
        assertEquals(6, innerData.get(5));
        assertEquals("d", nameVec.getObject(3).toString());
      }
    }
  }
}
