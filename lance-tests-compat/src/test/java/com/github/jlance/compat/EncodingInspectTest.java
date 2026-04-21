package com.github.jlance.compat;

import com.github.jlance.format.LanceFileReader;
import com.github.jlance.format.decoder.PageDecoder;
import java.nio.file.Path;
import java.nio.file.Paths;
import lance.encodings.EncodingsV20.ArrayEncoding;
import lance.file.v2.File2.ColumnMetadata;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;

public class EncodingInspectTest {

  @Test
  public void inspectM22Data() throws Exception {
    inspectDir(Paths.get("..", "compat_tests", "data", "milestone_22", "test_count_rows", "data"));
  }

  @Test
  public void inspectVariousPatterns() throws Exception {
    String[] patterns = {"small_ints", "bool_col", "repeated", "uint8_small"};
    for (String pattern : patterns) {
      Path dir = Paths.get("..", "compat_tests", "data", "test_enc_" + pattern, "data");
      if (java.nio.file.Files.isDirectory(dir)) {
        System.out.println("\n========== Pattern: " + pattern + " ==========");
        inspectDir(dir);
      }
    }
  }

  @Test
  public void inspectVersionPatterns() throws Exception {
    String[] patterns = {"large_bool", "small_ints_v21", "repeated_v21"};
    String[] versions = {"2.0", "2.1", "2.2", "2.3", "next"};
    for (String pattern : patterns) {
      for (String version : versions) {
        Path dir = Paths.get("..", "compat_tests", "data", "test_enc_" + pattern + "_" + version, "data");
        if (java.nio.file.Files.isDirectory(dir)) {
          System.out.println("\n========== Pattern: " + pattern + " / Version: " + version + " ==========");
          inspectDir(dir);
        }
      }
    }
  }

  private void inspectDir(Path dataDir) throws Exception {
    java.nio.file.DirectoryStream<Path> stream = java.nio.file.Files.newDirectoryStream(dataDir, "*.lance");
    for (Path file : stream) {
      System.out.println("=== File: " + file.getFileName() + " ===");
      try (BufferAllocator allocator = new RootAllocator();
           LanceFileReader reader = new LanceFileReader(file)) {
        var footer = reader.readFooter();
        for (int col = 0; col < footer.getColumnMetadatas().size(); col++) {
          ColumnMetadata cm = footer.getColumnMetadatas().get(col);
          System.out.println("Column " + col + ": pages=" + cm.getPagesCount());
          for (int p = 0; p < cm.getPagesCount(); p++) {
            var page = cm.getPages(p);
            ArrayEncoding enc = PageDecoder.unpackArrayEncoding(page.getEncoding());
            System.out.println("  Page " + p + ": len=" + page.getLength() + ", encoding=" + formatEncoding(enc));
          }
        }
      }
    }
  }

  private static String formatEncoding(ArrayEncoding enc) {
    if (enc == null) return "null";
    if (enc.hasFlat()) return "Flat bits=" + enc.getFlat().getBitsPerValue();
    if (enc.hasNullable()) {
      var n = enc.getNullable();
      if (n.hasNoNulls()) return "Nullable(NoNulls -> " + formatEncoding(n.getNoNulls().getValues()) + ")";
      if (n.hasSomeNulls()) return "Nullable(SomeNulls -> " + formatEncoding(n.getSomeNulls().getValues()) + ")";
      if (n.hasAllNulls()) return "Nullable(AllNulls)";
      return "Nullable(?)";
    }
    if (enc.hasBitpacked()) return "Bitpacked compressed=" + enc.getBitpacked().getCompressedBitsPerValue();
    if (enc.hasRle()) return "Rle bits=" + enc.getRle().getBitsPerValue();
    if (enc.hasConstant()) return "Constant";
    if (enc.hasBinary()) return "Binary";
    if (enc.hasDictionary()) return "Dictionary";
    if (enc.hasFixedSizeList()) return "FixedSizeList";
    if (enc.hasList()) return "List";
    if (enc.hasStruct()) return "Struct";
    if (enc.hasFixedSizeBinary()) return "FixedSizeBinary";
    if (enc.hasByteStreamSplit()) return "ByteStreamSplit";
    if (enc.hasPackedStruct()) return "PackedStruct";
    if (enc.hasFsst()) return "Fsst";
    return "Unknown:" + enc.getArrayEncodingCase();
  }
}
