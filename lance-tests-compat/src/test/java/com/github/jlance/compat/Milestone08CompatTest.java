package com.github.jlance.compat;

import static org.junit.jupiter.api.Assertions.*;

import com.github.jlance.format.LanceFileReader;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

public class Milestone08CompatTest {

  private static Path findLanceFile(String name) throws Exception {
    Path base = Paths.get("..", "compat_tests", "data", "milestone_08", name, "data");
    try (var stream = Files.list(base)) {
      return stream.filter(p -> p.toString().endsWith(".lance")).findFirst().orElseThrow();
    }
  }

  @Test
  public void testDecimalSinglePage() throws Exception {
    Path file = findLanceFile("test_decimal");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(5, root.getRowCount());

        DecimalVector dec128 = (DecimalVector) root.getVector("dec128");
        assertEquals(new BigDecimal("0.00"), dec128.getObject(0));
        assertEquals(new BigDecimal("123.45"), dec128.getObject(1));
        assertEquals(new BigDecimal("-999.99"), dec128.getObject(2));
        assertTrue(dec128.isNull(3));
        assertEquals(new BigDecimal("98765.43"), dec128.getObject(4));

        Decimal256Vector dec256 = (Decimal256Vector) root.getVector("dec256");
        assertEquals(new BigDecimal("0.000000"), dec256.getObject(0));
        assertEquals(new BigDecimal("12345.678901"), dec256.getObject(1));
        assertEquals(new BigDecimal("-999999999999.999999"), dec256.getObject(2));
        assertTrue(dec256.isNull(3));
        assertEquals(new BigDecimal("12345678901234567890.123456"), dec256.getObject(4));
      }
    }
  }

  @Test
  public void testDecimal128MultiPage() throws Exception {
    Path file = findLanceFile("test_decimal128_multi_page");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(100_000, root.getRowCount());

        DecimalVector dec = (DecimalVector) root.getVector("dec128_multi");
        assertEquals(18, dec.getPrecision());
        assertEquals(4, dec.getScale());
        assertFalse(dec.isNull(0));
        assertFalse(dec.isNull(99999));
      }
    }
  }
}
