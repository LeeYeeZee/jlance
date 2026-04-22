// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.compat;

import com.github.jlance.format.LanceFileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

public class DebugM44 {
  public static void main(String[] args) throws Exception {
    Path file = Paths.get("compat_tests", "data", "milestone_44", "test_nested_3layer_all_null.lance");
    try (BufferAllocator allocator = new RootAllocator();
         LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        System.out.println("Row count: " + root.getRowCount());
      }
    }
  }
}
