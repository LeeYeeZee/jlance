// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.compat;

import com.github.jlance.format.LanceFileReader;
import com.github.jlance.format.LanceFileFooter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import lance.file.v2.File2.ColumnMetadata;
import lance.encodings.EncodingsV20.ArrayEncoding;
import org.junit.jupiter.api.Test;

public class Milestone14InspectTest {

  private static Path findLanceFile(String name) throws Exception {
    Path base = Paths.get("..", "compat_tests", "data", "milestone_14", name, "data");
    try (var stream = Files.list(base)) {
      return stream.filter(p -> p.toString().endsWith(".lance")).findFirst().orElseThrow();
    }
  }

  @Test
  public void inspectConstantInt32() throws Exception {
    Path file = findLanceFile("test_constant_int32");
    try (LanceFileReader reader = new LanceFileReader(file)) {
      LanceFileFooter footer = reader.readFooter();
      for (int i = 0; i < footer.getColumnMetadatas().size(); i++) {
        ColumnMetadata cm = footer.getColumnMetadatas().get(i);
        System.out.println("Column " + i + ":");
        for (ColumnMetadata.Page page : cm.getPagesList()) {
          ArrayEncoding enc = com.github.jlance.format.decoder.PageDecoder.unpackArrayEncoding(page.getEncoding());
          if (enc != null) {
            System.out.println("  encoding case: " + enc.getArrayEncodingCase());
            if (enc.hasNullable()) {
              var nullable = enc.getNullable();
              System.out.println("  nullable: " + nullable.getNullabilityCase());
              if (nullable.hasNoNulls()) {
                inspectInner(nullable.getNoNulls().getValues(), "  ");
              }
            } else {
              inspectInner(enc, "  ");
            }
          }
        }
      }
    }
  }

  @Test
  public void inspectAllNullInt32() throws Exception {
    Path file = findLanceFile("test_all_null_int32");
    try (LanceFileReader reader = new LanceFileReader(file)) {
      LanceFileFooter footer = reader.readFooter();
      for (int i = 0; i < footer.getColumnMetadatas().size(); i++) {
        ColumnMetadata cm = footer.getColumnMetadatas().get(i);
        System.out.println("Column " + i + ":");
        for (ColumnMetadata.Page page : cm.getPagesList()) {
          ArrayEncoding enc = com.github.jlance.format.decoder.PageDecoder.unpackArrayEncoding(page.getEncoding());
          if (enc != null) {
            System.out.println("  encoding case: " + enc.getArrayEncodingCase());
            if (enc.hasNullable()) {
              var nullable = enc.getNullable();
              System.out.println("  nullable: " + nullable.getNullabilityCase());
            }
          }
        }
      }
    }
  }

  private void inspectInner(ArrayEncoding enc, String indent) {
    System.out.println(indent + "inner case: " + enc.getArrayEncodingCase());
    if (enc.hasConstant()) {
      System.out.println(indent + "CONSTANT value hex: " + bytesToHex(enc.getConstant().getValue().toByteArray()));
    }
    if (enc.hasFlat()) {
      System.out.println(indent + "FLAT bits=" + enc.getFlat().getBitsPerValue());
    }
  }

  private static String bytesToHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes) {
      sb.append(String.format("%02x", b));
    }
    return sb.toString();
  }
}
