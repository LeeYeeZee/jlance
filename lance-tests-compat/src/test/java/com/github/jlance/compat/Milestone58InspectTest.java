// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.compat;

import com.github.jlance.format.LanceFileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.junit.jupiter.api.Test;

public class Milestone58InspectTest {

  private static final Path DATA_DIR = Paths.get("..", "compat_tests", "data", "milestone_58");

  @Test
  public void inspect() throws Exception {
    Path file = DATA_DIR.resolve("test_mixed_nested_5.lance");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        System.out.println("Schema: " + root.getSchema());
        System.out.println("Row count: " + root.getRowCount());
        ListVector outerList = (ListVector) root.getVector("nested");
        StructVector outerStruct = (StructVector) outerList.getDataVector();
        ListVector midList = (ListVector) outerStruct.getChild("items");
        StructVector midStruct = (StructVector) midList.getDataVector();
        ListVector innerList = (ListVector) midStruct.getChild("vals");
        IntVector intVec = (IntVector) innerList.getDataVector();

        for (int row = 0; row < root.getRowCount(); row++) {
          System.out.println("=== Row " + row + " ===");
          System.out.println("outerList null=" + outerList.isNull(row) + ", count=" + (outerList.getElementEndIndex(row) - outerList.getElementStartIndex(row)));
          for (int i = outerList.getElementStartIndex(row); i < outerList.getElementEndIndex(row); i++) {
            System.out.println("  outerStruct[" + i + "] null=" + outerStruct.isNull(i));
            int midCount = midList.getElementEndIndex(i) - midList.getElementStartIndex(i);
            System.out.println("    midList count=" + midCount);
            for (int j = midList.getElementStartIndex(i); j < midList.getElementEndIndex(i); j++) {
              System.out.println("      midStruct[" + j + "] null=" + midStruct.isNull(j));
              int innerCount = innerList.getElementEndIndex(j) - innerList.getElementStartIndex(j);
              System.out.println("        innerList count=" + innerCount);
              for (int k = innerList.getElementStartIndex(j); k < innerList.getElementEndIndex(j); k++) {
                System.out.println("          int[" + k + "]=" + intVec.get(k));
              }
            }
          }
        }
      }
    }
  }
}
