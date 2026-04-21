package com.github.jlance.compat;

import static org.junit.jupiter.api.Assertions.*;

import com.github.jlance.format.LanceFileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

/**
 * Compatibility tests for Milestone 25: Null type support.
 */
public class Milestone25CompatTest {

  private static Path dataPath(String name) {
    return Paths.get("..", "compat_tests", "data", "milestone_25", name);
  }

  @Test
  public void testNullColumn() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(dataPath("test_null.lance"))) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(5, root.getRowCount());
        NullVector vec = (NullVector) root.getVector("null_col");
        assertEquals(5, vec.getValueCount());
        for (int i = 0; i < 5; i++) {
          assertTrue(vec.isNull(i));
        }
      }
    }
  }

  @Test
  public void testNullMixed() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(dataPath("test_null_mixed.lance"))) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(5, root.getRowCount());
        IntVector idVec = (IntVector) root.getVector("id");
        NullVector nullVec = (NullVector) root.getVector("always_null");
        VarCharVector nameVec = (VarCharVector) root.getVector("name");
        for (int i = 0; i < 5; i++) {
          assertEquals(i + 1, idVec.get(i));
          assertTrue(nullVec.isNull(i));
          assertEquals(String.valueOf((char) ('a' + i)), new String(nameVec.get(i)));
        }
      }
    }
  }
}
