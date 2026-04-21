package com.github.jlance.compat;

import com.github.jlance.format.LanceDatasetReader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import lance.table.Table.Manifest;
import lance.table.Table.DataFragment;
import lance.table.Table.DataFile;
import org.junit.jupiter.api.Test;

public class DebugM21 {
  @Test
  public void debug() throws Exception {
    Path versionsDir = Paths.get("..", "compat_tests", "data", "milestone_21", "test_schema_evolution", "_versions");
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(versionsDir)) {
      for (Path path : stream) {
        System.out.println("=== " + path.getFileName() + " ===");
        Manifest manifest = LanceDatasetReader.readManifestFile(path);
        System.out.println("Fragments: " + manifest.getFragmentsCount());
        System.out.println("Fields: " + manifest.getFieldsList());
        for (DataFragment fragment : manifest.getFragmentsList()) {
          System.out.println("  Fragment id: " + fragment.getId());
          System.out.println("    Files: " + fragment.getFilesCount());
          for (DataFile df : fragment.getFilesList()) {
            System.out.println("      " + df.getPath());
          }
          System.out.println("    Has deletion: " + fragment.hasDeletionFile());
        }
      }
    }
  }
}
