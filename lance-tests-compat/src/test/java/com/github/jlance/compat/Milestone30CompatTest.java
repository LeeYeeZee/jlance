package com.github.jlance.compat;

import static org.junit.jupiter.api.Assertions.*;

import com.github.jlance.format.LanceFileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.Test;

/**
 * Compatibility tests for Milestone 30: Dictionary with small index types.
 */
public class Milestone30CompatTest {

  private static Path findLanceFile(String name) throws Exception {
    Path base = Paths.get("..", "compat_tests", "data", "milestone_30", name, "data");
    try (var stream = Files.list(base)) {
      return stream.filter(p -> p.toString().endsWith(".lance")).findFirst().orElseThrow();
    }
  }

  @Test
  public void testDictionarySmallIndex() throws Exception {
    Path file = findLanceFile("test_dict_small_index");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(6, root.getRowCount());

        VarCharVector strVec = (VarCharVector) root.getVector("dict_int8_str");
        assertEquals(new Text("a"), strVec.getObject(0));
        assertEquals(new Text("b"), strVec.getObject(1));
        assertEquals(new Text("a"), strVec.getObject(2));
        assertEquals(new Text("c"), strVec.getObject(3));
        assertTrue(strVec.isNull(4));
        assertEquals(new Text("b"), strVec.getObject(5));

        IntVector intVec = (IntVector) root.getVector("dict_int16_int");
        assertEquals(10, intVec.get(0));
        assertEquals(20, intVec.get(1));
        assertEquals(10, intVec.get(2));
        assertEquals(30, intVec.get(3));
        assertTrue(intVec.isNull(4));
        assertEquals(20, intVec.get(5));
      }
    }
  }
}
