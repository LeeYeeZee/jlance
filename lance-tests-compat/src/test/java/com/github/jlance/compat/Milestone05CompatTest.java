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
import org.apache.arrow.vector.complex.StructVector;
import org.junit.jupiter.api.Test;

public class Milestone05CompatTest {

  private static Path findLanceFile(String name) throws Exception {
    Path base = Paths.get("..", "compat_tests", "data", "milestone_05", name, "data");
    try (var stream = Files.list(base)) {
      return stream.filter(p -> p.toString().endsWith(".lance")).findFirst().orElseThrow();
    }
  }

  @Test
  public void testInt32MultiPage() throws Exception {
    Path file = findLanceFile("test_int32_multi_page");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(100000, root.getRowCount());
        IntVector vec = (IntVector) root.getVector("val");
        assertNotNull(vec);
        assertEquals(100000, vec.getValueCount());
        // Spot-check a few values
        assertEquals(0, vec.get(0));
        assertEquals(99999, vec.get(99999));
        assertEquals(50000, vec.get(50000));
        assertEquals(255, vec.get(255));
        assertEquals(256, vec.get(256));
      }
    }
  }

  @Test
  public void testStringMultiPage() throws Exception {
    Path file = findLanceFile("test_string_multi_page");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(10000, root.getRowCount());
        VarCharVector vec = (VarCharVector) root.getVector("val");
        assertNotNull(vec);
        assertEquals(10000, vec.getValueCount());
        assertEquals("row_0", new String(vec.get(0), java.nio.charset.StandardCharsets.UTF_8));
        assertEquals("row_9999", new String(vec.get(9999), java.nio.charset.StandardCharsets.UTF_8));
        assertEquals("row_5000", new String(vec.get(5000), java.nio.charset.StandardCharsets.UTF_8));
      }
    }
  }

  @Test
  public void testStructMultiPage() throws Exception {
    Path file = findLanceFile("test_struct_multi_page");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(100000, root.getRowCount());
        StructVector vec = (StructVector) root.getVector("s");
        assertNotNull(vec);
        assertEquals(100000, vec.getValueCount());

        IntVector xVec = (IntVector) vec.getChild("x");
        org.apache.arrow.vector.Float4Vector yVec = (org.apache.arrow.vector.Float4Vector) vec.getChild("y");
        assertNotNull(xVec);
        assertNotNull(yVec);

        assertEquals(0, xVec.get(0));
        assertEquals(0.0f, yVec.get(0), 0.001f);
        assertEquals(99999, xVec.get(99999));
        assertEquals(99999.0f, yVec.get(99999), 0.001f);
        assertEquals(50000, xVec.get(50000));
        assertEquals(50000.0f, yVec.get(50000), 0.001f);
      }
    }
  }
}
