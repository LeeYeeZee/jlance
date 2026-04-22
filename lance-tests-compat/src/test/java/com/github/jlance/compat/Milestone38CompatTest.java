// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.compat;

import static org.junit.jupiter.api.Assertions.*;

import com.github.jlance.format.LanceFileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

/**
 * Compatibility tests for Milestone 38: V2.1 MiniBlockLayout with Dictionary.
 */
public class Milestone38CompatTest {

  private static final Path DATA_DIR = Paths.get("..", "compat_tests", "data", "milestone_38");

  @Test
  public void testDictionary() throws Exception {
    Path file = DATA_DIR.resolve("test_dictionary.lance");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(10, root.getRowCount());

        // dict_str: dictionary<string, int32>
        VarCharVector dictStr = (VarCharVector) root.getVector("dict_str");
        assertEquals(10, dictStr.getValueCount());
        assertEquals("alpha", new String(dictStr.get(0)));
        assertEquals("beta", new String(dictStr.get(1)));
        assertEquals("gamma", new String(dictStr.get(2)));
        assertEquals("alpha", new String(dictStr.get(3)));
        assertEquals("delta", new String(dictStr.get(4)));
        assertTrue(dictStr.isNull(5), "dict_str[5] should be null");
        assertEquals("beta", new String(dictStr.get(6)));
        assertEquals("gamma", new String(dictStr.get(7)));
        assertEquals("alpha", new String(dictStr.get(8)));
        assertEquals("delta", new String(dictStr.get(9)));

        // dict_int: dictionary<int64, int32>
        BigIntVector dictInt = (BigIntVector) root.getVector("dict_int");
        assertEquals(10, dictInt.getValueCount());
        assertEquals(100L, dictInt.get(0));
        assertEquals(200L, dictInt.get(1));
        assertEquals(100L, dictInt.get(2));
        assertEquals(300L, dictInt.get(3));
        assertTrue(dictInt.isNull(4), "dict_int[4] should be null");
        assertEquals(200L, dictInt.get(5));
        assertEquals(100L, dictInt.get(6));
        assertEquals(300L, dictInt.get(7));
        assertEquals(200L, dictInt.get(8));
        assertEquals(100L, dictInt.get(9));

        // dict_small: dictionary<string, int32> (2 values, no nulls)
        VarCharVector dictSmall = (VarCharVector) root.getVector("dict_small");
        assertEquals(10, dictSmall.getValueCount());
        assertEquals("x", new String(dictSmall.get(0)));
        assertEquals("y", new String(dictSmall.get(1)));

        // dict_nullable: dictionary<string, int32> (nullable)
        VarCharVector dictNullable = (VarCharVector) root.getVector("dict_nullable");
        assertEquals(10, dictNullable.getValueCount());
        assertEquals("a", new String(dictNullable.get(0)));
        assertTrue(dictNullable.isNull(1), "dict_nullable[1] should be null");
        assertEquals("b", new String(dictNullable.get(2)));
        assertEquals("a", new String(dictNullable.get(3)));
        assertTrue(dictNullable.isNull(4), "dict_nullable[4] should be null");
        assertEquals("c", new String(dictNullable.get(5)));
        assertEquals("a", new String(dictNullable.get(6)));
        assertEquals("b", new String(dictNullable.get(7)));
        assertTrue(dictNullable.isNull(8), "dict_nullable[8] should be null");
        assertEquals("c", new String(dictNullable.get(9)));
      }
    }
  }
}
