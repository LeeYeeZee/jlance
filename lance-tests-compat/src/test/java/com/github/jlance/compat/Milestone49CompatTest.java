package com.github.jlance.compat;

import static org.junit.jupiter.api.Assertions.*;

import com.github.jlance.format.LanceFileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

/**
 * Compatibility tests for Milestone 49: FSST string compression in V2.1.
 */
public class Milestone49CompatTest {

  private static final Path DATA_DIR = Paths.get("..", "compat_tests", "data", "milestone_49");

  @Test
  public void testFsstAuto() throws Exception {
    Path file = DATA_DIR.resolve("test_fsst_auto.lance");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(5000, root.getRowCount());
        VarCharVector vec = (VarCharVector) root.getVector("name");
        assertNotNull(vec);
        assertEquals(5000, vec.getValueCount());
        assertEquals("Customer#000000001", vec.getObject(0).toString());
        assertEquals("Customer#000000002", vec.getObject(1).toString());
        assertEquals("Customer#000000001", vec.getObject(10).toString());
        assertEquals("Order#000000001", vec.getObject(5).toString());
      }
    }
  }

  @Test
  public void testFsstNullable() throws Exception {
    Path file = DATA_DIR.resolve("test_fsst_nullable.lance");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(220, root.getRowCount());
        VarCharVector vec = (VarCharVector) root.getVector("name");
        assertNotNull(vec);
        assertEquals(220, vec.getValueCount());
        // First 100 should be non-null
        assertFalse(vec.isNull(0));
        assertEquals("Customer#000000001", vec.getObject(0).toString());
        // Rows 100-119 are null
        assertTrue(vec.isNull(100));
        assertTrue(vec.isNull(119));
        // Row 120 onwards are non-null again
        assertFalse(vec.isNull(120));
      }
    }
  }

  @Test
  public void testFsstBinary() throws Exception {
    Path file = DATA_DIR.resolve("test_fsst_binary.lance");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(5000, root.getRowCount());
        VarBinaryVector vec = (VarBinaryVector) root.getVector("data");
        assertNotNull(vec);
        assertEquals(5000, vec.getValueCount());
        assertArrayEquals("Customer#000000001".getBytes("UTF-8"), vec.get(0));
        assertArrayEquals("Customer#000000002".getBytes("UTF-8"), vec.get(1));
      }
    }
  }
}
