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

/**
 * Compatibility test for struct&lt;list&lt;int&gt;, list&lt;string&gt;&gt;.
 */
public class Milestone52CompatTest {

  private static final Path DATA_DIR = Paths.get("..", "compat_tests", "data", "milestone_52");

  @Test
  public void testStructListList() throws Exception {
    Path file = DATA_DIR.resolve("test_struct_list_list.lance");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(5, root.getRowCount());

        StructVector structVec = (StructVector) root.getVector("record");
        assertNotNull(structVec);

        ListVector numbersList = (ListVector) structVec.getChild("numbers");
        ListVector namesList = (ListVector) structVec.getChild("names");
        IntVector numbersData = (IntVector) numbersList.getDataVector();
        VarCharVector namesData = (VarCharVector) namesList.getDataVector();

        // Row 0: numbers=[1,2,3], names=["a","b"]
        assertFalse(structVec.isNull(0));
        assertEquals(3, numbersList.getElementEndIndex(0) - numbersList.getElementStartIndex(0));
        assertEquals(1, numbersData.get(0));
        assertEquals(2, numbersData.get(1));
        assertEquals(3, numbersData.get(2));
        assertEquals(2, namesList.getElementEndIndex(0) - namesList.getElementStartIndex(0));
        assertEquals("a", namesData.getObject(0).toString());
        assertEquals("b", namesData.getObject(1).toString());

        // Row 1: numbers=[], names=["c"]
        assertFalse(structVec.isNull(1));
        assertEquals(0, numbersList.getElementEndIndex(1) - numbersList.getElementStartIndex(1));
        assertEquals(1, namesList.getElementEndIndex(1) - namesList.getElementStartIndex(1));
        assertEquals("c", namesData.getObject(2).toString());

        // Row 2: null struct
        assertTrue(structVec.isNull(2));

        // Row 3: numbers=[], names=[]
        assertFalse(structVec.isNull(3));
        assertEquals(0, numbersList.getElementEndIndex(3) - numbersList.getElementStartIndex(3));
        assertEquals(0, namesList.getElementEndIndex(3) - namesList.getElementStartIndex(3));

        // Row 4: numbers=null, names=["d","e","f"]
        assertFalse(structVec.isNull(4));
        assertEquals(0, numbersList.getElementEndIndex(4) - numbersList.getElementStartIndex(4));
        assertEquals(3, namesList.getElementEndIndex(4) - namesList.getElementStartIndex(4));
        assertEquals("d", namesData.getObject(3).toString());
        assertEquals("e", namesData.getObject(4).toString());
        assertEquals("f", namesData.getObject(5).toString());
      }
    }
  }
}
