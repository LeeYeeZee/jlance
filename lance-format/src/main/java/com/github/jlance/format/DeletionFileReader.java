// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.format;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Set;
import lance.table.Table.DeletionFile;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.roaringbitmap.RoaringBitmap;

/**
 * Reads Lance deletion files and returns the set of deleted row offsets.
 *
 * <p>Deletion files are stored in the dataset's {@code _deletions/} directory.
 * There are two formats:
 * <ul>
 *   <li>{@code .arrow} — Arrow IPC file with a single Int32Array column</li>
 *   <li>{@code .bin} — Serialized Roaring Bitmap</li>
 * </ul>
 */
public class DeletionFileReader {

  /**
   * Reads a deletion file and returns a set of deleted local row offsets.
   */
  public static Set<Integer> readDeletionFile(
      Path datasetPath, long fragmentId, DeletionFile deletionFile, BufferAllocator allocator)
      throws IOException {
    if (deletionFile == null) {
      return java.util.Collections.emptySet();
    }

    Path deletionsDir = datasetPath.resolve("_deletions");
    String extension =
        deletionFile.getFileType() == DeletionFile.DeletionFileType.ARROW_ARRAY
            ? ".arrow"
            : ".bin";
    String fileName =
        fragmentId
            + "-"
            + deletionFile.getReadVersion()
            + "-"
            + Long.toUnsignedString(deletionFile.getId())
            + extension;
    Path filePath = deletionsDir.resolve(fileName);

    switch (deletionFile.getFileType()) {
      case ARROW_ARRAY:
        return readArrowArray(filePath, allocator);
      case BITMAP:
        return readBitmap(filePath);
      default:
        throw new UnsupportedOperationException(
            "Unsupported deletion file type: " + deletionFile.getFileType());
    }
  }

  private static Set<Integer> readArrowArray(Path filePath, BufferAllocator allocator)
      throws IOException {
    Set<Integer> deletedRows = new HashSet<>();
    try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ);
        ArrowFileReader reader =
            new ArrowFileReader(
                channel, allocator, org.apache.arrow.compression.CommonsCompressionFactory.INSTANCE)) {
      reader.loadNextBatch();
      try (VectorSchemaRoot root = reader.getVectorSchemaRoot()) {
        FieldVector vec = root.getVector(0);
        for (int i = 0; i < vec.getValueCount(); i++) {
          deletedRows.add(((Number) vec.getObject(i)).intValue());
        }
      }
    }
    return deletedRows;
  }

  private static Set<Integer> readBitmap(Path filePath) throws IOException {
    RoaringBitmap bitmap = new RoaringBitmap();
    try (java.io.FileInputStream fis = new java.io.FileInputStream(filePath.toFile())) {
      bitmap.deserialize(new java.io.DataInputStream(fis));
    }
    Set<Integer> deletedRows = new HashSet<>();
    for (int value : bitmap) {
      deletedRows.add(value);
    }
    return deletedRows;
  }
}
