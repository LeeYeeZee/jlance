// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.format;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import lance.table.Table.DataFile;
import lance.table.Table.DataFragment;
import lance.table.Table.Manifest;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.VectorBatchAppender;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Reader for Lance dataset directories (table-level reading).
 *
 * <p>A Lance dataset is organized as a directory containing:
 * <ul>
 *   <li>{@code data/} — .lance data files</li>
 *   <li>{@code _versions/} — manifest files (one per version)</li>
 *   <li>{@code _transactions/} — transaction files</li>
 * </ul>
 */
public class LanceDatasetReader implements AutoCloseable {

  private final Path datasetPath;

  public LanceDatasetReader(Path datasetPath) {
    this.datasetPath = datasetPath;
  }

  /**
   * Reads the latest version of the dataset by finding the manifest file
   * in {@code _versions/} with the highest version number.
   */
  public VectorSchemaRoot readDataset(BufferAllocator allocator) throws IOException {
    return readDataset(allocator, null, false);
  }

  /**
   * Reads the latest version of the dataset with optional column projection.
   *
   * @param columns list of top-level field names to read; null or empty reads all columns
   */
  public VectorSchemaRoot readDataset(
      BufferAllocator allocator, java.util.List<String> columns) throws IOException {
    return readDataset(allocator, columns, false);
  }

  /**
   * Reads the latest version of the dataset with optional column projection and row IDs.
   *
   * @param columns list of top-level field names to read; null or empty reads all columns
   * @param withRowId if true, adds a {@code _rowid} column with global row identifiers
   */
  public VectorSchemaRoot readDataset(
      BufferAllocator allocator, java.util.List<String> columns, boolean withRowId)
      throws IOException {
    Path versionsDir = datasetPath.resolve("_versions");
    if (!Files.isDirectory(versionsDir)) {
      throw new IOException("Not a valid Lance dataset: missing _versions directory");
    }
    Path manifestPath = findLatestManifest(versionsDir);
    return readDatasetFromManifest(manifestPath, allocator, columns, withRowId);
  }

  /**
   * Reads a specific version of the dataset.
   *
   * @param version the dataset version number (1-based)
   */
  public VectorSchemaRoot readDataset(BufferAllocator allocator, long version)
      throws IOException {
    return readDataset(allocator, version, null, false);
  }

  /**
   * Reads a specific version of the dataset with optional column projection.
   *
   * @param version the dataset version number (1-based)
   * @param columns list of top-level field names to read; null or empty reads all columns
   */
  public VectorSchemaRoot readDataset(
      BufferAllocator allocator, long version, java.util.List<String> columns)
      throws IOException {
    return readDataset(allocator, version, columns, false);
  }

  /**
   * Reads a specific version of the dataset with optional column projection and row IDs.
   *
   * @param version the dataset version number (1-based)
   * @param columns list of top-level field names to read; null or empty reads all columns
   * @param withRowId if true, adds a {@code _rowid} column with global row identifiers
   */
  public VectorSchemaRoot readDataset(
      BufferAllocator allocator,
      long version,
      java.util.List<String> columns,
      boolean withRowId)
      throws IOException {
    Path versionsDir = datasetPath.resolve("_versions");
    if (!Files.isDirectory(versionsDir)) {
      throw new IOException("Not a valid Lance dataset: missing _versions directory");
    }
    Path manifestPath = findManifestByVersion(versionsDir, version);
    return readDatasetFromManifest(manifestPath, allocator, columns, withRowId);
  }

  /**
   * Returns the total number of rows in the latest version of the dataset.
   *
   * <p>This counts rows across all fragments, subtracting deleted rows.
   */
  public long countRows() throws IOException {
    Path versionsDir = datasetPath.resolve("_versions");
    if (!Files.isDirectory(versionsDir)) {
      throw new IOException("Not a valid Lance dataset: missing _versions directory");
    }
    Path manifestPath = findLatestManifest(versionsDir);
    return countRowsFromManifest(manifestPath);
  }

  /**
   * Returns the total number of rows in a specific version of the dataset.
   *
   * @param version the dataset version number (1-based)
   */
  public long countRows(long version) throws IOException {
    Path versionsDir = datasetPath.resolve("_versions");
    if (!Files.isDirectory(versionsDir)) {
      throw new IOException("Not a valid Lance dataset: missing _versions directory");
    }
    Path manifestPath = findManifestByVersion(versionsDir, version);
    return countRowsFromManifest(manifestPath);
  }

  /**
   * Reads specific rows by their global index from the latest version of the dataset.
   *
   * @param indices zero-based global row indices in the order they should appear in the result
   */
  public VectorSchemaRoot take(BufferAllocator allocator, int[] indices) throws IOException {
    return take(allocator, indices, null);
  }

  /**
   * Reads specific rows by their global index with optional column projection.
   *
   * @param indices zero-based global row indices in the order they should appear in the result
   * @param columns list of top-level field names to read; null or empty reads all columns
   */
  public VectorSchemaRoot take(
      BufferAllocator allocator, int[] indices, java.util.List<String> columns)
      throws IOException {
    Path versionsDir = datasetPath.resolve("_versions");
    if (!Files.isDirectory(versionsDir)) {
      throw new IOException("Not a valid Lance dataset: missing _versions directory");
    }
    Path manifestPath = findLatestManifest(versionsDir);
    return takeFromManifest(manifestPath, allocator, indices, columns);
  }

  /**
   * Reads specific rows by their global index from a specific version.
   *
   * @param version the dataset version number (1-based)
   * @param indices zero-based global row indices in the order they should appear in the result
   */
  public VectorSchemaRoot take(
      BufferAllocator allocator, long version, int[] indices) throws IOException {
    return take(allocator, version, indices, null);
  }

  /**
   * Reads specific rows by their global index from a specific version with column projection.
   *
   * @param version the dataset version number (1-based)
   * @param indices zero-based global row indices in the order they should appear in the result
   * @param columns list of top-level field names to read; null or empty reads all columns
   */
  public VectorSchemaRoot take(
      BufferAllocator allocator,
      long version,
      int[] indices,
      java.util.List<String> columns)
      throws IOException {
    Path versionsDir = datasetPath.resolve("_versions");
    if (!Files.isDirectory(versionsDir)) {
      throw new IOException("Not a valid Lance dataset: missing _versions directory");
    }
    Path manifestPath = findManifestByVersion(versionsDir, version);
    return takeFromManifest(manifestPath, allocator, indices, columns);
  }

  /**
   * Reads the first {@code n} rows from the latest version of the dataset.
   *
   * @param n maximum number of rows to read
   */
  public VectorSchemaRoot head(BufferAllocator allocator, int n) throws IOException {
    return head(allocator, n, null);
  }

  /**
   * Reads the first {@code n} rows from the latest version with column projection.
   *
   * @param n maximum number of rows to read
   * @param columns list of top-level field names to read; null or empty reads all columns
   */
  public VectorSchemaRoot head(
      BufferAllocator allocator, int n, java.util.List<String> columns) throws IOException {
    Path versionsDir = datasetPath.resolve("_versions");
    if (!Files.isDirectory(versionsDir)) {
      throw new IOException("Not a valid Lance dataset: missing _versions directory");
    }
    Path manifestPath = findLatestManifest(versionsDir);
    return headFromManifest(manifestPath, allocator, n, columns);
  }

  /**
   * Reads the first {@code n} rows from a specific version.
   *
   * @param version the dataset version number (1-based)
   * @param n maximum number of rows to read
   */
  public VectorSchemaRoot head(BufferAllocator allocator, long version, int n)
      throws IOException {
    return head(allocator, version, n, null);
  }

  /**
   * Reads the first {@code n} rows from a specific version with column projection.
   *
   * @param version the dataset version number (1-based)
   * @param n maximum number of rows to read
   * @param columns list of top-level field names to read; null or empty reads all columns
   */
  public VectorSchemaRoot head(
      BufferAllocator allocator,
      long version,
      int n,
      java.util.List<String> columns)
      throws IOException {
    Path versionsDir = datasetPath.resolve("_versions");
    if (!Files.isDirectory(versionsDir)) {
      throw new IOException("Not a valid Lance dataset: missing _versions directory");
    }
    Path manifestPath = findManifestByVersion(versionsDir, version);
    return headFromManifest(manifestPath, allocator, n, columns);
  }

  private VectorSchemaRoot headFromManifest(
      Path manifestPath,
      BufferAllocator allocator,
      int n,
      java.util.List<String> columns)
      throws IOException {
    Manifest manifest = readManifestFile(manifestPath);

    if (n <= 0 || manifest.getFragmentsCount() == 0) {
      java.util.List<org.apache.arrow.vector.types.pojo.Field> arrowFields =
          LanceSchemaConverter.convertFields(manifest.getFieldsList());
      if (columns != null && !columns.isEmpty()) {
        arrowFields.removeIf(f -> !columns.contains(f.getName()));
      }
      org.apache.arrow.vector.types.pojo.Schema schema =
          new org.apache.arrow.vector.types.pojo.Schema(arrowFields);
      return VectorSchemaRoot.create(schema, allocator);
    }

    int remaining = n;
    List<VectorSchemaRoot> fragmentRoots = new ArrayList<>();

    for (DataFragment fragment : manifest.getFragmentsList()) {
      if (remaining <= 0) {
        break;
      }

      VectorSchemaRoot root = readFragment(fragment, allocator, columns, manifest);
      int rootRows = root.getRowCount();

      if (rootRows <= remaining) {
        fragmentRoots.add(root);
        remaining -= rootRows;
      } else {
        // Slice to only first 'remaining' rows.
        List<FieldVector> slicedVectors = new ArrayList<>();
        for (FieldVector vec : root.getFieldVectors()) {
          slicedVectors.add(sliceVector(vec, 0, remaining, allocator));
        }
        VectorSchemaRoot sliced =
            new VectorSchemaRoot(root.getSchema().getFields(), slicedVectors, remaining);
        root.close();
        fragmentRoots.add(sliced);
        remaining = 0;
      }
    }

    if (fragmentRoots.isEmpty()) {
      java.util.List<org.apache.arrow.vector.types.pojo.Field> arrowFields =
          LanceSchemaConverter.convertFields(manifest.getFieldsList());
      if (columns != null && !columns.isEmpty()) {
        arrowFields.removeIf(f -> !columns.contains(f.getName()));
      }
      org.apache.arrow.vector.types.pojo.Schema schema =
          new org.apache.arrow.vector.types.pojo.Schema(arrowFields);
      return VectorSchemaRoot.create(schema, allocator);
    }

    if (fragmentRoots.size() == 1) {
      return fragmentRoots.get(0);
    }

    // Merge multiple fragments field by field.
    VectorSchemaRoot merged = fragmentRoots.get(0);
    for (int i = 1; i < fragmentRoots.size(); i++) {
      VectorSchemaRoot next = fragmentRoots.get(i);
      for (int col = 0; col < merged.getFieldVectors().size(); col++) {
        @SuppressWarnings("unchecked")
        org.apache.arrow.vector.ValueVector target = merged.getFieldVectors().get(col);
        @SuppressWarnings("unchecked")
        org.apache.arrow.vector.ValueVector source = next.getFieldVectors().get(col);
        VectorBatchAppender.batchAppend(target, source);
      }
      merged.setRowCount(merged.getRowCount() + next.getRowCount());
      next.close();
    }
    return merged;
  }

  private static FieldVector sliceVector(
      FieldVector source, int offset, int length, BufferAllocator allocator) {
    @SuppressWarnings("unchecked")
    FieldVector target = (FieldVector) source.getField().createVector(allocator);
    target.allocateNew();
    for (int i = 0; i < length; i++) {
      target.copyFromSafe(offset + i, i, source);
    }
    target.setValueCount(length);
    return target;
  }

  private VectorSchemaRoot takeFromManifest(
      Path manifestPath,
      BufferAllocator allocator,
      int[] indices,
      java.util.List<String> columns)
      throws IOException {
    Manifest manifest = readManifestFile(manifestPath);

    if (indices == null || indices.length == 0) {
      java.util.List<org.apache.arrow.vector.types.pojo.Field> arrowFields =
          LanceSchemaConverter.convertFields(manifest.getFieldsList());
      if (columns != null && !columns.isEmpty()) {
        arrowFields.removeIf(f -> !columns.contains(f.getName()));
      }
      org.apache.arrow.vector.types.pojo.Schema schema =
          new org.apache.arrow.vector.types.pojo.Schema(arrowFields);
      return VectorSchemaRoot.create(schema, allocator);
    }

    if (manifest.getFragmentsCount() == 0) {
      throw new IndexOutOfBoundsException(
          "Cannot take rows from empty dataset: indices length=" + indices.length);
    }

    // Read all fragments (with deletions applied).
    List<VectorSchemaRoot> fragmentRoots = new ArrayList<>();
    for (DataFragment fragment : manifest.getFragmentsList()) {
      fragmentRoots.add(readFragment(fragment, allocator, columns, manifest));
    }

    // Compute cumulative visible row counts.
    List<Long> cumulativeRows = new ArrayList<>();
    long cumulative = 0;
    for (VectorSchemaRoot root : fragmentRoots) {
      cumulative += root.getRowCount();
      cumulativeRows.add(cumulative);
    }

    // Validate indices.
    for (int idx : indices) {
      if (idx < 0 || idx >= cumulative) {
        for (VectorSchemaRoot root : fragmentRoots) {
          root.close();
        }
        throw new IndexOutOfBoundsException(
            "Index " + idx + " out of bounds for dataset with " + cumulative + " rows");
      }
    }

    // Determine result schema from first fragment.
    org.apache.arrow.vector.types.pojo.Schema resultSchema = fragmentRoots.get(0).getSchema();

    // Create result vectors with capacity for all requested rows.
    List<FieldVector> resultVectors = new ArrayList<>();
    for (org.apache.arrow.vector.types.pojo.Field field : resultSchema.getFields()) {
      @SuppressWarnings("unchecked")
      FieldVector vec = (FieldVector) field.createVector(allocator);
      vec.setInitialCapacity(indices.length);
      vec.allocateNew();
      resultVectors.add(vec);
    }

    // Extract rows preserving original index order.
    for (int globalIdx : indices) {
      int fragIdx = 0;
      while (fragIdx < cumulativeRows.size() && globalIdx >= cumulativeRows.get(fragIdx)) {
        fragIdx++;
      }
      long fragStart = (fragIdx == 0) ? 0 : cumulativeRows.get(fragIdx - 1);
      int localIdx = (int) (globalIdx - fragStart);

      VectorSchemaRoot root = fragmentRoots.get(fragIdx);
      for (int col = 0; col < root.getFieldVectors().size(); col++) {
        FieldVector source = root.getFieldVectors().get(col);
        FieldVector target = resultVectors.get(col);
        int dstIdx = target.getValueCount();
        target.copyFromSafe(localIdx, dstIdx, source);
        target.setValueCount(dstIdx + 1);
      }
    }

    // Clean up fragment roots.
    for (VectorSchemaRoot root : fragmentRoots) {
      root.close();
    }

    return new VectorSchemaRoot(resultSchema.getFields(), resultVectors, indices.length);
  }

  private long countRowsFromManifest(Path manifestPath) throws IOException {
    Manifest manifest = readManifestFile(manifestPath);
    long totalRows = 0;
    for (DataFragment fragment : manifest.getFragmentsList()) {
      long fragmentRows = fragment.getPhysicalRows();
      if (fragment.hasDeletionFile()) {
        fragmentRows -= fragment.getDeletionFile().getNumDeletedRows();
      }
      totalRows += fragmentRows;
    }
    return totalRows;
  }

  private VectorSchemaRoot readDatasetFromManifest(
      Path manifestPath,
      BufferAllocator allocator,
      java.util.List<String> columns,
      boolean withRowId)
      throws IOException {
    Manifest manifest = readManifestFile(manifestPath);

    if (manifest.getFragmentsCount() == 0) {
      // Empty dataset: build schema from manifest fields, zero rows.
      List<org.apache.arrow.vector.types.pojo.Field> arrowFields =
          LanceSchemaConverter.convertFields(manifest.getFieldsList());
      if (withRowId) {
        arrowFields.add(createRowIdField());
      }
      org.apache.arrow.vector.types.pojo.Schema schema =
          new org.apache.arrow.vector.types.pojo.Schema(arrowFields);
      return VectorSchemaRoot.create(schema, allocator);
    }

    // Read each fragment and merge.
    List<VectorSchemaRoot> fragmentRoots = new ArrayList<>();
    for (DataFragment fragment : manifest.getFragmentsList()) {
      VectorSchemaRoot fragmentRoot = readFragment(fragment, allocator, columns, manifest);
      if (withRowId) {
        fragmentRoot = addRowIdColumn(fragmentRoot, fragment.getId(), allocator);
      }
      fragmentRoots.add(fragmentRoot);
    }

    if (fragmentRoots.size() == 1) {
      return fragmentRoots.get(0);
    }

    // Merge multiple fragments field by field.
    VectorSchemaRoot merged = fragmentRoots.get(0);
    for (int i = 1; i < fragmentRoots.size(); i++) {
      VectorSchemaRoot next = fragmentRoots.get(i);
      for (int col = 0; col < merged.getFieldVectors().size(); col++) {
        @SuppressWarnings("unchecked")
        org.apache.arrow.vector.ValueVector target = merged.getFieldVectors().get(col);
        @SuppressWarnings("unchecked")
        org.apache.arrow.vector.ValueVector source = next.getFieldVectors().get(col);
        VectorBatchAppender.batchAppend(target, source);
      }
      merged.setRowCount(merged.getRowCount() + next.getRowCount());
      next.close();
    }
    return merged;
  }

  private VectorSchemaRoot readFragment(
      DataFragment fragment,
      BufferAllocator allocator,
      java.util.List<String> columns,
      Manifest manifest)
      throws IOException {
    Path dataDir = datasetPath.resolve("data");
    List<VectorSchemaRoot> fileRoots = new ArrayList<>();

    org.apache.arrow.vector.types.pojo.Schema fragmentSchema =
        new org.apache.arrow.vector.types.pojo.Schema(
            LanceSchemaConverter.convertFields(manifest.getFieldsList()));

    org.apache.arrow.vector.types.pojo.Schema targetSchema = fragmentSchema;
    if (columns != null && !columns.isEmpty()) {
      List<org.apache.arrow.vector.types.pojo.Field> projectedFields = new ArrayList<>();
      for (org.apache.arrow.vector.types.pojo.Field field : fragmentSchema.getFields()) {
        if (columns.contains(field.getName())) {
          projectedFields.add(field);
        }
      }
      targetSchema = new org.apache.arrow.vector.types.pojo.Schema(projectedFields);
    }

    for (DataFile dataFile : fragment.getFilesList()) {
      Path filePath = dataDir.resolve(dataFile.getPath());
      try (LanceFileReader reader = new LanceFileReader(filePath)) {
        VectorSchemaRoot root = reader.readBatch(allocator, columns);
        fileRoots.add(root);
      }
    }

    if (fileRoots.isEmpty()) {
      throw new IOException("Fragment has no data files");
    }

    VectorSchemaRoot merged;
    if (fileRoots.size() == 1) {
      merged = alignSchema(fileRoots.get(0), targetSchema, allocator);
    } else {
      // Schema evolution: multiple data files form a single fragment
      // by column-wise union. All files must have the same row count.
      int rowCount = fileRoots.get(0).getRowCount();
      for (VectorSchemaRoot root : fileRoots) {
        if (root.getRowCount() != rowCount) {
          throw new IOException(
              "Data files in fragment have mismatched row counts: "
                  + rowCount
                  + " vs "
                  + root.getRowCount());
        }
      }

      java.util.Set<String> targetNames = new java.util.HashSet<>();
      for (org.apache.arrow.vector.types.pojo.Field field : targetSchema.getFields()) {
        targetNames.add(field.getName());
      }
      java.util.Map<String, FieldVector> vectorsByName = new java.util.HashMap<>();
      for (VectorSchemaRoot root : fileRoots) {
        for (FieldVector vec : root.getFieldVectors()) {
          if (targetNames.contains(vec.getName())) {
            vectorsByName.computeIfAbsent(vec.getName(), k -> copyVector(vec, allocator));
          }
        }
        root.close();
      }

      List<FieldVector> mergedVectors = new ArrayList<>();
      for (org.apache.arrow.vector.types.pojo.Field field : targetSchema.getFields()) {
        FieldVector vec = vectorsByName.get(field.getName());
        if (vec != null) {
          mergedVectors.add(vec);
        } else {
          FieldVector nullVec = (FieldVector) field.createVector(allocator);
          nullVec.setInitialCapacity(rowCount);
          nullVec.allocateNew();
          for (int i = 0; i < rowCount; i++) {
            nullVec.setNull(i);
          }
          nullVec.setValueCount(rowCount);
          mergedVectors.add(nullVec);
        }
      }
      merged = new VectorSchemaRoot(targetSchema.getFields(), mergedVectors, rowCount);
    }

    // Apply deletion file if present.
    if (fragment.hasDeletionFile()) {
      java.util.Set<Integer> deletedRows =
          DeletionFileReader.readDeletionFile(
              datasetPath, fragment.getId(), fragment.getDeletionFile(), allocator);
      if (!deletedRows.isEmpty()) {
        merged = applyDeletions(merged, deletedRows, allocator);
      }
    }

    return merged;
  }

  private static VectorSchemaRoot applyDeletions(
      VectorSchemaRoot source,
      java.util.Set<Integer> deletedRows,
      BufferAllocator allocator) {
    int sourceRows = source.getRowCount();
    int targetRows = sourceRows - deletedRows.size();
    if (targetRows <= 0) {
      source.close();
      List<FieldVector> emptyVectors = new ArrayList<>();
      for (Field field : source.getSchema().getFields()) {
        @SuppressWarnings("unchecked")
        FieldVector emptyVec = (FieldVector) field.createVector(allocator);
        emptyVec.allocateNew();
        emptyVectors.add(emptyVec);
      }
      return new VectorSchemaRoot(source.getSchema().getFields(), emptyVectors, 0);
    }

    List<FieldVector> filteredVectors = new ArrayList<>();
    for (FieldVector vector : source.getFieldVectors()) {
      FieldVector target = (FieldVector) vector.getField().createVector(allocator);
      target.allocateNew();
      int dst = 0;
      for (int src = 0; src < sourceRows; src++) {
        if (!deletedRows.contains(src)) {
          target.copyFromSafe(src, dst, vector);
          dst++;
        }
      }
      target.setValueCount(dst);
      filteredVectors.add(target);
    }
    source.close();
    return new VectorSchemaRoot(source.getSchema().getFields(), filteredVectors, targetRows);
  }

  private static org.apache.arrow.vector.types.pojo.Field createRowIdField() {
    return new org.apache.arrow.vector.types.pojo.Field(
        "_rowid",
        new org.apache.arrow.vector.types.pojo.FieldType(
            false,
            new org.apache.arrow.vector.types.pojo.ArrowType.Int(64, false),
            null,
            null),
        null);
  }

  private static VectorSchemaRoot addRowIdColumn(
      VectorSchemaRoot source, long fragmentId, BufferAllocator allocator) {
    int rowCount = source.getRowCount();
    org.apache.arrow.vector.types.pojo.Field rowIdField = createRowIdField();
    org.apache.arrow.vector.UInt8Vector rowIdVec =
        (org.apache.arrow.vector.UInt8Vector) rowIdField.createVector(allocator);
    rowIdVec.allocateNew();
    for (int i = 0; i < rowCount; i++) {
      long rowId = (fragmentId << 32) | (i & 0xFFFFFFFFL);
      rowIdVec.set(i, rowId);
    }
    rowIdVec.setValueCount(rowCount);

    List<org.apache.arrow.vector.types.pojo.Field> fields =
        new ArrayList<>(source.getSchema().getFields());
    fields.add(rowIdField);

    List<FieldVector> vectors = new ArrayList<>(source.getFieldVectors());
    vectors.add(rowIdVec);

    return new VectorSchemaRoot(fields, vectors, rowCount);
  }

  /**
   * Aligns a data file's VectorSchemaRoot to the fragment's manifest schema.
   *
   * <p>Columns present in the data file are copied. Missing columns are filled with null
   * vectors. Column order follows the target schema. The source root is closed.
   */
  private static VectorSchemaRoot alignSchema(
      VectorSchemaRoot source,
      org.apache.arrow.vector.types.pojo.Schema targetSchema,
      BufferAllocator allocator) {
    int rowCount = source.getRowCount();
    List<FieldVector> alignedVectors = new ArrayList<>();
    for (org.apache.arrow.vector.types.pojo.Field field : targetSchema.getFields()) {
      FieldVector vec = source.getVector(field.getName());
      if (vec != null) {
        alignedVectors.add(copyVector(vec, allocator));
      } else {
        FieldVector nullVec = (FieldVector) field.createVector(allocator);
        nullVec.setInitialCapacity(rowCount);
        nullVec.allocateNew();
        for (int i = 0; i < rowCount; i++) {
          nullVec.setNull(i);
        }
        nullVec.setValueCount(rowCount);
        alignedVectors.add(nullVec);
      }
    }
    source.close();
    return new VectorSchemaRoot(targetSchema.getFields(), alignedVectors, rowCount);
  }

  private static FieldVector copyVector(FieldVector source, BufferAllocator allocator) {
    FieldVector target = (FieldVector) source.getField().createVector(allocator);
    target.allocateNew();
    int count = source.getValueCount();
    for (int i = 0; i < count; i++) {
      target.copyFromSafe(i, i, source);
    }
    target.setValueCount(count);
    return target;
  }

  /**
   * Finds the manifest file with the highest version number in the _versions directory.
   */
  private static Path findLatestManifest(Path versionsDir) throws IOException {
    Path latest = null;
    long latestVersion = 0;
    boolean found = false;

    try (DirectoryStream<Path> stream = Files.newDirectoryStream(versionsDir)) {
      for (Path path : stream) {
        String name = path.getFileName().toString();
        if (!name.endsWith(".manifest")) {
          continue;
        }
        String versionStr = name.substring(0, name.length() - ".manifest".length());
        try {
          long version = Long.parseUnsignedLong(versionStr);
          if (!found || Long.compareUnsigned(version, latestVersion) < 0) {
            latestVersion = version;
            latest = path;
            found = true;
          }
        } catch (NumberFormatException e) {
          // Skip files with non-numeric names
        }
      }
    }

    if (!found) {
      throw new IOException("No manifest file found in " + versionsDir);
    }
    return latest;
  }

  /**
   * Finds the manifest file for a specific dataset version.
   *
   * <p>Lance manifest filenames are {@code (u64_max - version).manifest}.
   */
  private static Path findManifestByVersion(Path versionsDir, long version) throws IOException {
    long filenameValue = -1L - version;
    String filename = Long.toUnsignedString(filenameValue) + ".manifest";
    Path path = versionsDir.resolve(filename);
    if (!Files.exists(path)) {
      throw new IOException("Manifest not found for version " + version + ": " + filename);
    }
    return path;
  }

  /**
   * Reads a manifest file following the Lance format.
   *
   * <p>Layout:
   * <pre>
   *   [protobuf bytes] [length: u32 LE] [manifest_pos: i64 LE] [magic suffix]
   * </pre>
   */
  public static Manifest readManifestFile(Path path) throws IOException {
    byte[] data = Files.readAllBytes(path);
    if (data.length < 16) {
      throw new IOException("Manifest file too small: " + data.length);
    }

    // Verify magic (last 4 bytes should be "LANC")
    boolean hasMagic =
        data[data.length - 4] == 'L'
            && data[data.length - 3] == 'A'
            && data[data.length - 2] == 'N'
            && data[data.length - 1] == 'C';
    if (!hasMagic) {
      throw new IOException("Invalid manifest: magic number not found");
    }

    long manifestPos =
        ByteBuffer.wrap(data, data.length - 16, 8)
            .order(ByteOrder.LITTLE_ENDIAN)
            .getLong();
    int manifestLen = data.length - (int) manifestPos;

    ByteBuffer manifestBuf = ByteBuffer.wrap(data, (int) manifestPos, manifestLen);
    manifestBuf.order(ByteOrder.LITTLE_ENDIAN);

    int recordedLength = manifestBuf.getInt();
    int protobufLen = manifestLen - 4 - 16;
    if (protobufLen != recordedLength) {
      throw new IOException(
          "Manifest length mismatch: recorded="
              + recordedLength
              + ", actual="
              + protobufLen);
    }

    byte[] protobufBytes = new byte[protobufLen];
    manifestBuf.get(protobufBytes);
    return Manifest.parseFrom(protobufBytes);
  }

  @Override
  public void close() throws IOException {
    // Nothing to close at dataset level; file readers are per-read.
  }
}
