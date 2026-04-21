package com.github.jlance.compat;

import static org.junit.jupiter.api.Assertions.*;

import com.github.jlance.format.LanceDatasetReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;

public class Milestone22CompatTest {

  @Test
  public void testCountRowsLatestVersion() throws Exception {
    Path datasetPath =
        Paths.get("..", "compat_tests", "data", "milestone_22", "test_count_rows");
    try (LanceDatasetReader reader = new LanceDatasetReader(datasetPath)) {
      assertEquals(120L, reader.countRows());
    }
  }

  @Test
  public void testCountRowsVersion1() throws Exception {
    Path datasetPath =
        Paths.get("..", "compat_tests", "data", "milestone_22", "test_count_rows");
    try (LanceDatasetReader reader = new LanceDatasetReader(datasetPath)) {
      assertEquals(100L, reader.countRows(1L));
    }
  }

  @Test
  public void testCountRowsVersion2WithDeletions() throws Exception {
    Path datasetPath =
        Paths.get("..", "compat_tests", "data", "milestone_22", "test_count_rows");
    try (LanceDatasetReader reader = new LanceDatasetReader(datasetPath)) {
      assertEquals(70L, reader.countRows(2L));
    }
  }

  @Test
  public void testCountRowsVersion3WithAppend() throws Exception {
    Path datasetPath =
        Paths.get("..", "compat_tests", "data", "milestone_22", "test_count_rows");
    try (LanceDatasetReader reader = new LanceDatasetReader(datasetPath)) {
      assertEquals(120L, reader.countRows(3L));
    }
  }

  @Test
  public void testCountRowsEmptyDataset() throws Exception {
    Path datasetPath =
        Paths.get("..", "compat_tests", "data", "milestone_22", "test_empty");
    try (LanceDatasetReader reader = new LanceDatasetReader(datasetPath)) {
      assertEquals(0L, reader.countRows());
    }
  }
}
