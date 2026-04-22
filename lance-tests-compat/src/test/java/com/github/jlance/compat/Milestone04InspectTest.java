// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.compat;

import com.github.jlance.format.LanceFileFooter;
import com.github.jlance.format.LanceFileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import lance.encodings.EncodingsV20.ArrayEncoding;
import lance.file.v2.File2.ColumnMetadata;
import org.junit.jupiter.api.Test;

public class Milestone04InspectTest {

  private static void inspect(String name) throws Exception {
    Path base = Paths.get("..", "compat_tests", "data", "milestone_04", name, "data");
    Path[] files;
    try (var stream = Files.list(base)) {
      files = stream.filter(p -> p.toString().endsWith(".lance")).toArray(Path[]::new);
    }
    Path lanceFile = files[0];

    try (LanceFileReader reader = new LanceFileReader(lanceFile)) {
      LanceFileFooter footer = reader.readFooter();
      System.out.println("=== " + name + " ===");
      System.out.println("Columns: " + footer.getNumColumns());
      for (int i = 0; i < footer.getNumColumns(); i++) {
        ColumnMetadata cm = footer.getColumnMetadatas().get(i);
        System.out.println("Column " + i + ":");
        System.out.println("  pages: " + cm.getPagesCount());
        System.out.println("  column encoding type: " + getEncodingType(cm.getEncoding()));
        for (int p = 0; p < cm.getPagesCount(); p++) {
          var page = cm.getPages(p);
          System.out.println("  Page " + p + ":");
          System.out.println("    length: " + page.getLength());
          System.out.println("    buffers: " + page.getBufferOffsetsCount());
          System.out.println("    encoding type: " + getEncodingType(page.getEncoding()));
          ArrayEncoding ae = unpackArrayEncoding(page.getEncoding());
          if (ae != null) {
            printArrayEncoding(ae, 6);
          }
        }
      }
    }
  }

  private static String getEncodingType(lance.file.v2.File2.Encoding enc) {
    if (enc.hasNone()) return "none";
    if (enc.hasDirect()) return "direct";
    return "indirect";
  }

  private static ArrayEncoding unpackArrayEncoding(lance.file.v2.File2.Encoding enc) {
    if (!enc.hasDirect()) return null;
    try {
      com.google.protobuf.Any any =
          com.google.protobuf.Any.parseFrom(enc.getDirect().getEncoding());
      return any.unpack(ArrayEncoding.class);
    } catch (Exception e) {
      return null;
    }
  }

  private static void printArrayEncoding(ArrayEncoding ae, int indent) {
    String pad = " ".repeat(indent);
    switch (ae.getArrayEncodingCase()) {
      case FLAT:
        var flat = ae.getFlat();
        System.out.println(pad + "Flat bits_per_value=" + flat.getBitsPerValue());
        break;
      case NULLABLE:
        var nullable = ae.getNullable();
        System.out.println(pad + "Nullable");
        if (nullable.hasNoNulls()) {
          System.out.println(pad + "  NoNulls");
          printArrayEncoding(nullable.getNoNulls().getValues(), indent + 4);
        } else if (nullable.hasSomeNulls()) {
          System.out.println(pad + "  SomeNulls");
          System.out.println(pad + "  validity:");
          printArrayEncoding(nullable.getSomeNulls().getValidity(), indent + 4);
          System.out.println(pad + "  values:");
          printArrayEncoding(nullable.getSomeNulls().getValues(), indent + 4);
        } else if (nullable.hasAllNulls()) {
          System.out.println(pad + "  AllNulls");
        }
        break;
      case STRUCT:
        System.out.println(pad + "SimpleStruct");
        break;
      case FIXED_SIZE_LIST:
        var fsl = ae.getFixedSizeList();
        System.out.println(pad + "FixedSizeList dim=" + fsl.getDimension() + " has_validity=" + fsl.getHasValidity());
        printArrayEncoding(fsl.getItems(), indent + 2);
        break;
      case LIST:
        System.out.println(pad + "List");
        break;
      case BINARY:
        var bin = ae.getBinary();
        System.out.println(pad + "Binary");
        System.out.println(pad + "  indices:");
        printArrayEncoding(bin.getIndices(), indent + 4);
        System.out.println(pad + "  bytes:");
        printArrayEncoding(bin.getBytes(), indent + 4);
        break;
      case DICTIONARY:
        System.out.println(pad + "Dictionary");
        break;
      default:
        System.out.println(pad + "Unknown: " + ae.getArrayEncodingCase());
    }
  }

  @Test
  public void inspectAll() throws Exception {
    inspect("test_string");
    inspect("test_binary");
    inspect("test_bool");
    inspect("test_fixed_size_list");
  }
}
