// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.format;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import com.github.jlance.format.buffer.BufferReader;
import com.github.jlance.format.buffer.PageBufferStore;
import com.github.jlance.format.decoder.ArrayDecoder;
import com.github.jlance.format.decoder.ConstantLayoutDecoder;
import com.github.jlance.format.decoder.DecodedArray;
import com.github.jlance.format.decoder.MiniBlockLayoutDecoder;
import com.github.jlance.format.decoder.PageDecoder;
import com.github.jlance.format.decoder.StructuralListDecodeTask;
import lance.encodings.EncodingsV20.ArrayEncoding;
import lance.encodings21.EncodingsV21.PageLayout;
import lance.encodings21.EncodingsV21.RepDefLayer;
import lance.file.File.FileDescriptor;
import lance.file.v2.File2.ColumnMetadata;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.VectorBatchAppender;

/**
 * Reader for Lance v2 file format. Currently supports parsing the footer and column metadata.
 */
public class LanceFileReader implements AutoCloseable {
  private static final byte[] MAGIC = new byte[] {'L', 'A', 'N', 'C'};
  private static final int FOOTER_SIZE = 40; // bytes

  private final FileChannel channel;
  private final long fileSize;

  public LanceFileReader(Path path) throws IOException {
    this.channel = FileChannel.open(path, StandardOpenOption.READ);
    this.fileSize = channel.size();
    if (fileSize < FOOTER_SIZE) {
      throw new IOException("File too small to be a valid Lance file: " + fileSize);
    }
  }

  /**
   * Reads and parses the file footer, including column metadata offset table and global buffer
   * offset table. Also parses each ColumnMetadata protobuf message.
   */
  public LanceFileFooter readFooter() throws IOException {
    ByteBuffer footerBuf = ByteBuffer.allocate(FOOTER_SIZE);
    footerBuf.order(ByteOrder.LITTLE_ENDIAN);
    channel.read(footerBuf, fileSize - FOOTER_SIZE);
    footerBuf.flip();

    long columnMetadataOffset = footerBuf.getLong();
    long cmoTableOffset = footerBuf.getLong();
    long gboTableOffset = footerBuf.getLong();
    int numGlobalBuffers = footerBuf.getInt();
    int numColumns = footerBuf.getInt();
    int majorVersion = footerBuf.getShort() & 0xFFFF;
    int minorVersion = footerBuf.getShort() & 0xFFFF;

    byte[] magic = new byte[4];
    footerBuf.get(magic);
    if (!java.util.Arrays.equals(magic, MAGIC)) {
      throw new IOException(
          "Invalid Lance file magic: expected LANC, got " + new String(magic, java.nio.charset.StandardCharsets.UTF_8));
    }

    // Read Column Metadata Offset Table
    List<LanceFileFooter.ColumnMetadataOffset> columnMetadataOffsets =
        new ArrayList<>(numColumns);
    if (numColumns > 0) {
      ByteBuffer cmoBuf = ByteBuffer.allocate(numColumns * 16);
      cmoBuf.order(ByteOrder.LITTLE_ENDIAN);
      channel.read(cmoBuf, cmoTableOffset);
      cmoBuf.flip();
      for (int i = 0; i < numColumns; i++) {
        long position = cmoBuf.getLong();
        long size = cmoBuf.getLong();
        columnMetadataOffsets.add(new LanceFileFooter.ColumnMetadataOffset(position, size));
      }
    }

    // Read Global Buffer Offset Table
    List<LanceFileFooter.GlobalBufferOffset> globalBufferOffsets =
        new ArrayList<>(numGlobalBuffers);
    if (numGlobalBuffers > 0) {
      ByteBuffer gboBuf = ByteBuffer.allocate(numGlobalBuffers * 16);
      gboBuf.order(ByteOrder.LITTLE_ENDIAN);
      channel.read(gboBuf, gboTableOffset);
      gboBuf.flip();
      for (int i = 0; i < numGlobalBuffers; i++) {
        long position = gboBuf.getLong();
        long size = gboBuf.getLong();
        globalBufferOffsets.add(new LanceFileFooter.GlobalBufferOffset(position, size));
      }
    }

    // Parse each ColumnMetadata protobuf
    List<ColumnMetadata> columnMetadatas = new ArrayList<>(numColumns);
    for (LanceFileFooter.ColumnMetadataOffset cmo : columnMetadataOffsets) {
      ByteBuffer cmBuf = ByteBuffer.allocate((int) cmo.size());
      cmBuf.order(ByteOrder.LITTLE_ENDIAN);
      channel.read(cmBuf, cmo.position());
      cmBuf.flip();
      ColumnMetadata cm = ColumnMetadata.parseFrom(cmBuf);
      columnMetadatas.add(cm);
    }

    return new LanceFileFooter(
        majorVersion,
        minorVersion,
        numColumns,
        numGlobalBuffers,
        columnMetadataOffset,
        cmoTableOffset,
        gboTableOffset,
        columnMetadataOffsets,
        globalBufferOffsets,
        columnMetadatas);
  }

  /**
   * Reads a global buffer by index.
   */
  public byte[] readGlobalBuffer(int index) throws IOException {
    LanceFileFooter footer = readFooter();
    List<LanceFileFooter.GlobalBufferOffset> gbos = footer.getGlobalBufferOffsets();
    if (index < 0 || index >= gbos.size()) {
      throw new IndexOutOfBoundsException(
          "Global buffer index " + index + " out of bounds (" + gbos.size() + ")");
    }
    LanceFileFooter.GlobalBufferOffset gbo = gbos.get(index);
    ByteBuffer buf = ByteBuffer.allocate((int) gbo.size());
    buf.order(ByteOrder.LITTLE_ENDIAN);
    channel.read(buf, gbo.position());
    buf.flip();
    byte[] bytes = new byte[buf.remaining()];
    buf.get(bytes);
    return bytes;
  }

  /**
   * Reads the file descriptor (schema + row count) from global buffer 0.
   */
  public LanceFileMetadata readMetadata() throws IOException {
    byte[] schemaBytes = readGlobalBuffer(0);
    FileDescriptor fd = FileDescriptor.parseFrom(schemaBytes);
    long numRows = fd.getLength();
    List<org.apache.arrow.vector.types.pojo.Field> arrowFields =
        LanceSchemaConverter.convertFields(fd.getSchema().getFieldsList());
    Schema arrowSchema = new Schema(arrowFields);
    return new LanceFileMetadata(arrowSchema, numRows);
  }

  public long getFileSize() {
    return fileSize;
  }

  /**
   * Reads a single batch (all rows, all columns) from the file.
   *
   * <p>Currently supports single-page columns and SimpleStruct nested types.
   */
  public VectorSchemaRoot readBatch(BufferAllocator allocator) throws IOException {
    LanceFileFooter footer = readFooter();
    LanceFileMetadata metadata = readMetadata();
    Schema schema = metadata.getSchema();
    long numRows = metadata.getNumRows();

    List<FieldVector> vectors = new ArrayList<>();
    int[] nextColIndex = {0};
    for (Field field : schema.getFields()) {
      FieldVector vector = readField(field, nextColIndex, footer, (int) numRows, allocator).vector;
      vectors.add(vector);
    }

    VectorSchemaRoot root = new VectorSchemaRoot(schema.getFields(), vectors, (int) numRows);

    return root;
  }

  /**
   * Reads a single batch with column projection.
   *
   * @param columns list of top-level field names to read; if empty reads all columns
   */
  public VectorSchemaRoot readBatch(BufferAllocator allocator, java.util.List<String> columns)
      throws IOException {
    if (columns == null || columns.isEmpty()) {
      return readBatch(allocator);
    }
    java.util.Set<String> projection = new java.util.HashSet<>(columns);
    VectorSchemaRoot fullRoot = readBatch(allocator);
    try {
      int rowCount = fullRoot.getRowCount();
      java.util.List<Field> projectedFields = new ArrayList<>();
      java.util.List<FieldVector> projectedVectors = new ArrayList<>();
      for (int i = 0; i < fullRoot.getFieldVectors().size(); i++) {
        FieldVector vector = fullRoot.getFieldVectors().get(i);
        if (projection.contains(vector.getName())) {
          projectedFields.add(fullRoot.getSchema().getFields().get(i));
          projectedVectors.add(sliceVector(vector, 0, rowCount, allocator));
        }
      }
      return new VectorSchemaRoot(projectedFields, projectedVectors, rowCount);
    } finally {
      fullRoot.close();
    }
  }

  /**
   * Reads a subset of rows from the file.
   *
   * @param offset zero-based starting row
   * @param limit maximum number of rows to read
   */
  public VectorSchemaRoot readBatch(BufferAllocator allocator, int offset, int limit)
      throws IOException {
    VectorSchemaRoot fullRoot = readBatch(allocator);
    try {
      int totalRows = fullRoot.getRowCount();
      if (offset < 0) {
        offset = 0;
      }
      if (offset >= totalRows) {
        List<FieldVector> emptyVectors = new ArrayList<>();
        for (Field field : fullRoot.getSchema().getFields()) {
          @SuppressWarnings("unchecked")
          FieldVector emptyVec = (FieldVector) field.createVector(allocator);
          emptyVec.allocateNew();
          emptyVectors.add(emptyVec);
        }
        return new VectorSchemaRoot(fullRoot.getSchema().getFields(), emptyVectors, 0);
      }
      int actualLimit = Math.min(limit, totalRows - offset);

      List<FieldVector> slicedVectors = new ArrayList<>();
      for (FieldVector vector : fullRoot.getFieldVectors()) {
        FieldVector sliced = sliceVector(vector, offset, actualLimit, allocator);
        slicedVectors.add(sliced);
      }

      return new VectorSchemaRoot(
          fullRoot.getSchema().getFields(), slicedVectors, actualLimit);
    } finally {
      fullRoot.close();
    }
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

  private DecodedArray readField(
      Field field,
      int[] nextColIndex,
      LanceFileFooter footer,
      int numRows,
      BufferAllocator allocator)
      throws IOException {
    return readField(field, nextColIndex, footer, numRows, allocator, null, null, null, null);
  }

  private DecodedArray readField(
      Field field,
      int[] nextColIndex,
      LanceFileFooter footer,
      int numRows,
      BufferAllocator allocator,
      short[] parentRepLevels,
      short[] parentDefLevels,
      List<RepDefLayer> parentLayers)
      throws IOException {
    return readField(field, nextColIndex, footer, numRows, allocator,
        parentRepLevels, parentDefLevels, parentLayers, null);
  }

  private DecodedArray readField(
      Field field,
      int[] nextColIndex,
      LanceFileFooter footer,
      int numRows,
      BufferAllocator allocator,
      short[] parentRepLevels,
      short[] parentDefLevels,
      List<RepDefLayer> parentLayers,
      java.util.List<RepDefUnraveler.UnravelResult> pendingListResults)
      throws IOException {
    if (field.getType() instanceof ArrowType.Struct) {
      // V2.1+ struct: children are separate columns, struct itself does not consume
      // a column index. V2.0 SimpleStruct: struct consumes one column index.
      // Packed struct / blob: treated as a single primitive column even in V2.1.
      boolean isPackedOrBlob = isPackedStructField(field) || isBlobField(field);
      if (!footer.isV2_1OrLater() || isPackedOrBlob) {
        ColumnMetadata cm = footer.getColumnMetadatas().get(nextColIndex[0]);
        nextColIndex[0]++;
        if (!footer.isV2_1OrLater() && isPackedStructColumn(cm, false)) {
          return decodePrimitiveColumn(cm, field, footer, numRows, allocator);
        }
        if (isPackedOrBlob) {
          return decodePrimitiveColumn(cm, field, footer, numRows, allocator);
        }
      }
      return buildStructVector(field, nextColIndex, footer, numRows, allocator,
          parentRepLevels, parentDefLevels, parentLayers, pendingListResults);
    }

    if (field.getType() instanceof ArrowType.List || field.getType() instanceof ArrowType.LargeList) {
      if (footer.isV2_1OrLater()) {
        PageLayout layout = PageDecoder.unpackPageLayout(
            footer.getColumnMetadatas().get(nextColIndex[0]).getPages(0).getEncoding());
        if (layout != null) {
          java.util.List<RepDefLayer> layers;
          if (layout.hasMiniBlockLayout()) {
            layers = layout.getMiniBlockLayout().getLayersList();
          } else if (layout.hasConstantLayout()) {
            layers = layout.getConstantLayout().getLayersList();
          } else {
            layers = java.util.Collections.emptyList();
          }
          int listLayersInColumn = 0;
          for (RepDefLayer layer : layers) {
            if (isListLayer(layer)) {
              listLayersInColumn++;
            }
          }
          int listLayersInField = countListLayersDeep(field);
          Field itemField = field.getChildren().get(0);
          if (listLayersInColumn < listLayersInField) {
            // Multi-column list: fall back to V2.0 decodeListColumn path
            ColumnMetadata cm = footer.getColumnMetadatas().get(nextColIndex[0]);
            nextColIndex[0]++;
            FieldVector vector = decodeListColumn(cm, field, nextColIndex, footer, numRows, allocator);
            return new DecodedArray(vector);
          }
          if (pendingListResults != null && !pendingListResults.isEmpty()) {
            // Consume the outermost pending list result (last in the list).
            RepDefUnraveler.UnravelResult result = pendingListResults.remove(
                pendingListResults.size() - 1);
            int itemCount = result.offsets[result.offsets.length - 1];
            DecodedArray itemArray = readField(itemField, nextColIndex, footer, itemCount,
                allocator, parentRepLevels, parentDefLevels, parentLayers, pendingListResults);
            FieldVector listVec = buildListVector(
                itemArray.vector, field, result, allocator);
            return new DecodedArray(listVec, itemArray.repLevels, itemArray.defLevels,
                itemArray.layers, itemArray.listResults);
          }
          DecodedArray array = decodeV21ListColumn(field, nextColIndex, footer, numRows, allocator,
              parentRepLevels, parentDefLevels, parentLayers, pendingListResults);
          if (array.listResults != null && !array.listResults.isEmpty()) {
            FieldVector currentVec = array.vector;
            for (int i = 0; i < array.listResults.size(); i++) {
              int depthFromInner = array.listResults.size() - 1 - i;
              Field currentField = getFieldAtDepth(field, depthFromInner);
              currentVec = buildListVector(currentVec, currentField, array.listResults.get(i), allocator);
            }
            return new DecodedArray(currentVec, array.repLevels, array.defLevels, array.layers);
          }
          return array;
        }
      }
      ColumnMetadata cm = footer.getColumnMetadatas().get(nextColIndex[0]);
      nextColIndex[0]++;
      FieldVector vector = decodeListColumn(cm, field, nextColIndex, footer, numRows, allocator);
      return new DecodedArray(vector);
    }

    ColumnMetadata cm = footer.getColumnMetadatas().get(nextColIndex[0]);
    nextColIndex[0]++;
    return decodePrimitiveColumn(cm, field, footer, numRows, allocator);
  }

  private DecodedArray buildStructVector(
      Field field,
      int[] nextColIndex,
      LanceFileFooter footer,
      int numRows,
      BufferAllocator allocator)
      throws IOException {
    return buildStructVector(field, nextColIndex, footer, numRows, allocator, null, null, null, null);
  }

  private DecodedArray buildStructVector(
      Field field,
      int[] nextColIndex,
      LanceFileFooter footer,
      int numRows,
      BufferAllocator allocator,
      short[] parentRepLevels,
      short[] parentDefLevels,
      List<RepDefLayer> parentLayers)
      throws IOException {
    return buildStructVector(field, nextColIndex, footer, numRows, allocator,
        parentRepLevels, parentDefLevels, parentLayers, null);
  }

  private DecodedArray buildStructVector(
      Field field,
      int[] nextColIndex,
      LanceFileFooter footer,
      int numRows,
      BufferAllocator allocator,
      short[] parentRepLevels,
      short[] parentDefLevels,
      List<RepDefLayer> parentLayers,
      java.util.List<RepDefUnraveler.UnravelResult> pendingListResults)
      throws IOException {
    StructVector struct = (StructVector) field.createVector(allocator);
    struct.setInitialCapacity(numRows);
    struct.allocateNew();
    for (int i = 0; i < numRows; i++) {
      org.apache.arrow.vector.BitVectorHelper.setValidityBit(struct.getValidityBuffer(), i, 1);
    }

    boolean isV21 = footer.isV2_1OrLater();
    boolean isNullableStruct = isV21 && field.isNullable();
    short[] structDefLevels = null;
    int structLayerIndex = -1;
    List<DecodedArray> childArrays = new ArrayList<>();

    for (int ci = 0; ci < field.getChildren().size(); ci++) {
      Field childField = field.getChildren().get(ci);
      DecodedArray childArray = readField(childField, nextColIndex, footer, numRows, allocator,
          parentRepLevels, parentDefLevels, parentLayers, pendingListResults);
      childArrays.add(childArray);
      FieldVector childVec = childArray.vector;
      @SuppressWarnings("unchecked")
      FieldVector slot =
          struct.addOrGet(
              childField.getName(), childField.getFieldType(),
              (Class<? extends FieldVector>) childVec.getClass());
      childVec.makeTransferPair(slot).transfer();
      childVec.close();
      // If the child consumed rep/def (e.g. a list), subsequent children should
      // receive the original un-truncated rep/def.  Re-clone for each child.
      if (parentRepLevels != null && childArray.repLevels != null
          && childArray.repLevels != parentRepLevels) {
        parentRepLevels = parentRepLevels.clone();
        if (parentDefLevels != null) {
          parentDefLevels = parentDefLevels.clone();
        }
      }
    }

    // V2.1+ nullable struct: derive validity from first child's rep/def levels.
    if (isNullableStruct && !childArrays.isEmpty()) {
      DecodedArray firstChild = childArrays.get(0);
      if (firstChild.defLevels != null && firstChild.defLevels.length > 0
          && firstChild.layers != null) {
        short[] def = firstChild.defLevels;
        int nullStructLevel = computeOuterNullItemLevel(firstChild.layers);
        if (nullStructLevel >= 0) {
          if (firstChild.repLevels != null && firstChild.repLevels.length > 0) {
            // List child: rep > 0 marks a new row.  Check the first entry of each row.
            short[] rep = firstChild.repLevels;
            int entryIdx = 0;
            for (int row = 0; row < numRows && entryIdx < rep.length; row++) {
              boolean structNull = (def[entryIdx] == nullStructLevel);
              org.apache.arrow.vector.BitVectorHelper.setValidityBit(
                  struct.getValidityBuffer(), row, structNull ? 0 : 1);
              // Advance to the start of the next row.
              entryIdx++;
              while (entryIdx < rep.length && rep[entryIdx] == 0) {
                entryIdx++;
              }
            }
          } else {
            // Primitive child (no list layer)
            for (int i = 0; i < numRows && i < def.length; i++) {
              boolean structNull = (def[i] == nullStructLevel);
              org.apache.arrow.vector.BitVectorHelper.setValidityBit(
                  struct.getValidityBuffer(), i, structNull ? 0 : 1);
            }
          }
        }
      }
    }

    struct.setValueCount(numRows);
    return new DecodedArray(struct);
  }

  private DecodedArray decodePrimitiveColumn(
      ColumnMetadata cm, Field field, LanceFileFooter footer, int numRows, BufferAllocator allocator)
      throws IOException {
    PageDecoder decoder = new PageDecoder(channel, allocator);
    boolean isV21 = footer.isV2_1OrLater();
    if (cm.getPagesCount() == 0) {
      throw new IllegalStateException("Column " + field.getName() + " has no pages");
    }
    if (cm.getPagesCount() == 1) {
      return decoder.decodePageWithRepDef(cm.getPages(0), field, numRows, isV21);
    }

    // Multi-page column: decode each page and append into a single vector.
    @SuppressWarnings("unchecked")
    FieldVector target = (FieldVector) field.createVector(allocator);
    target.allocateNew();

    @SuppressWarnings("unchecked")
    List<ValueVector> pageVectors = new ArrayList<>();
    List<DecodedArray> pageArrays = new ArrayList<>();
    for (ColumnMetadata.Page page : cm.getPagesList()) {
      int pageRows = (int) page.getLength();
      DecodedArray pageArray = decoder.decodePageWithRepDef(page, field, pageRows, isV21);
      pageArrays.add(pageArray);
      pageVectors.add(pageArray.vector);
    }

    @SuppressWarnings("unchecked")
    FieldVector[] sources = pageVectors.toArray(new FieldVector[0]);
    VectorBatchAppender.batchAppend(target, sources);

    for (ValueVector v : pageVectors) {
      v.close();
    }
    target.setValueCount(target.getValueCount());

    // Merge rep/def state across pages
    short[] mergedRep = mergeShortArrays(pageArrays, a -> a.repLevels);
    short[] mergedDef = mergeShortArrays(pageArrays, a -> a.defLevels);
    List<RepDefLayer> layers = pageArrays.isEmpty() ? null : pageArrays.get(0).layers;
    return new DecodedArray(target, mergedRep, mergedDef, layers);
  }

  private static short[] mergeShortArrays(
      List<DecodedArray> arrays, java.util.function.Function<DecodedArray, short[]> extractor) {
    int total = 0;
    for (DecodedArray a : arrays) {
      short[] arr = extractor.apply(a);
      if (arr != null) {
        total += arr.length;
      }
    }
    if (total == 0) {
      return null;
    }
    short[] result = new short[total];
    int pos = 0;
    for (DecodedArray a : arrays) {
      short[] arr = extractor.apply(a);
      if (arr != null) {
        System.arraycopy(arr, 0, result, pos, arr.length);
        pos += arr.length;
      }
    }
    return result;
  }

  private FieldVector decodeListColumn(
      ColumnMetadata cm,
      Field field,
      int[] nextColIndex,
      LanceFileFooter footer,
      int numRows,
      BufferAllocator allocator)
      throws IOException {
    // V2.1+ list: encoded with PageLayout (MiniBlockLayout/FullZipLayout) using rep/def levels.
    // This path is normally handled in readField() before decodeListColumn is called.
    if (footer.isV2_1OrLater()) {
      PageLayout layout = PageDecoder.unpackPageLayout(cm.getPages(0).getEncoding());
      if (layout != null) {
        throw new IllegalStateException(
            "V2.1 list should be decoded via decodeV21ListColumn from readField");
      }
    }

    // V2.0 list: encoded with ArrayEncoding.List using offsets sub-encoding.
    List<Long> allOffsets = new ArrayList<>();
    long nullOffsetAdjustment = -1;

    for (ColumnMetadata.Page page : cm.getPagesList()) {
      ArrayEncoding encoding = PageDecoder.unpackArrayEncoding(page.getEncoding());
      if (encoding == null || !encoding.hasList()) {
        throw new IllegalStateException(
            "Expected List encoding for column " + field.getName());
      }
      var listEnc = encoding.getList();
      nullOffsetAdjustment = listEnc.getNullOffsetAdjustment();

      // Read page buffers
      List<byte[]> buffers = new ArrayList<>();
      for (int i = 0; i < page.getBufferOffsetsCount(); i++) {
        long offset = page.getBufferOffsets(i);
        long size = page.getBufferSizes(i);
        ByteBuffer buf = BufferReader.readBuffer(channel, offset, size);
        byte[] data = new byte[(int) size];
        buf.get(data);
        buffers.add(data);
      }
      // page buffers consumed
      PageBufferStore store = new PageBufferStore(buffers);

      // Decode the offsets array (described by listEnc.getOffsets())
      ArrayEncoding offsetsEncoding = listEnc.getOffsets();
      ArrayDecoder offsetsDecoder = PageDecoder.createDecoder(offsetsEncoding);
      Field tempField =
          new Field(
              "offsets", FieldType.nullable(new ArrowType.Int(64, true)), null);
      FieldVector offsetsVec =
          offsetsDecoder.decode(
              offsetsEncoding, (int) page.getLength(), store, tempField, allocator);
      org.apache.arrow.vector.BigIntVector bigIntVec = (org.apache.arrow.vector.BigIntVector) offsetsVec;
      for (int i = 0; i < bigIntVec.getValueCount(); i++) {
        allOffsets.add(bigIntVec.get(i));
      }
      bigIntVec.close();
    }

    if (allOffsets.size() != numRows) {
      throw new IllegalStateException(
          "List offsets count (" + allOffsets.size() + ") does not match expected rows (" + numRows + ")");
    }

    // 2. Convert Lance offsets to Arrow offsets.
    long[] arrowOffsets = new long[numRows + 1];
    arrowOffsets[0] = 0;
    for (int i = 0; i < numRows; i++) {
      long lanceOffset = allOffsets.get(i);
      if (lanceOffset >= nullOffsetAdjustment) {
        arrowOffsets[i + 1] = lanceOffset - nullOffsetAdjustment;
      } else {
        arrowOffsets[i + 1] = lanceOffset;
      }
    }
    int totalItems = (int) arrowOffsets[numRows];

    // 3. Recursively read the item child column.
    if (field.getChildren().isEmpty()) {
      throw new IllegalStateException("List field has no children: " + field.getName());
    }
    Field itemField = field.getChildren().get(0);
    FieldVector itemVec =
        readField(itemField, nextColIndex, footer, totalItems, allocator).vector;

    // 4. Build the Arrow ListVector or LargeListVector.
    FieldVector vector = field.createVector(allocator);
    if (vector instanceof ListVector) {
      ListVector listVec = (ListVector) vector;
      listVec.setInitialCapacity(numRows);
      listVec.allocateNew();

      org.apache.arrow.memory.ArrowBuf offsetBuf = listVec.getOffsetBuffer();
      for (int i = 0; i <= numRows; i++) {
        offsetBuf.setInt(i * 4, (int) arrowOffsets[i]);
      }
      for (int i = 0; i < numRows; i++) {
        long lanceOffset = allOffsets.get(i);
        if (lanceOffset >= nullOffsetAdjustment) {
          listVec.setNull(i);
        } else {
          listVec.setNotNull(i);
        }
      }

      org.apache.arrow.vector.ValueVector dataVec =
          listVec.addOrGetVector(itemField.getFieldType()).getVector();
      itemVec.makeTransferPair(dataVec).transfer();
      itemVec.close();

      listVec.setLastSet(numRows - 1);
      listVec.setValueCount(numRows);
      return listVec;
    }
    if (vector instanceof LargeListVector) {
      LargeListVector listVec = (LargeListVector) vector;
      listVec.setInitialCapacity(numRows);
      listVec.allocateNew();

      org.apache.arrow.memory.ArrowBuf offsetBuf = listVec.getOffsetBuffer();
      for (int i = 0; i <= numRows; i++) {
        offsetBuf.setLong(i * 8, arrowOffsets[i]);
      }
      for (int i = 0; i < numRows; i++) {
        long lanceOffset = allOffsets.get(i);
        if (lanceOffset >= nullOffsetAdjustment) {
          listVec.setNull(i);
        } else {
          listVec.setNotNull(i);
        }
      }

      org.apache.arrow.vector.ValueVector dataVec =
          listVec.addOrGetVector(itemField.getFieldType()).getVector();
      itemVec.makeTransferPair(dataVec).transfer();
      itemVec.close();

      listVec.setLastSet(numRows - 1);
      listVec.setValueCount(numRows);
      return listVec;
    }
    throw new UnsupportedOperationException(
        "Unsupported list vector type: " + vector.getClass().getName());
  }

  /**
   * Decodes a V2.1+ list column using repetition/definition levels.
   *
   * <p>V2.1 list columns use {@link PageLayout} (e.g. MiniBlockLayout) with rep/def
   * levels instead of the V2.0 {@code ArrayEncoding.List} offsets wrapper.
   *
   * <p>The list itself does not consume a column index in V2.1; the first child column
   * carries the rep/def for this list (and any inner lists).  Struct items are handled
   * by recursively reading child columns.
   */
  private DecodedArray decodeV21ListColumn(
      Field field,
      int[] nextColIndex,
      LanceFileFooter footer,
      int numRows,
      BufferAllocator allocator)
      throws IOException {
    return decodeV21ListColumn(field, nextColIndex, footer, numRows, allocator, null, null, null, null);
  }

  private DecodedArray decodeV21ListColumn(
      Field field,
      int[] nextColIndex,
      LanceFileFooter footer,
      int numRows,
      BufferAllocator allocator,
      short[] parentRepLevels,
      short[] parentDefLevels,
      List<RepDefLayer> parentLayers)
      throws IOException {
    return decodeV21ListColumn(field, nextColIndex, footer, numRows, allocator,
        parentRepLevels, parentDefLevels, parentLayers, null);
  }

  private DecodedArray decodeV21ListColumn(
      Field field,
      int[] nextColIndex,
      LanceFileFooter footer,
      int numRows,
      BufferAllocator allocator,
      short[] parentRepLevels,
      short[] parentDefLevels,
      List<RepDefLayer> parentLayers,
      java.util.List<RepDefUnraveler.UnravelResult> pendingListResults)
      throws IOException {
    if (field.getChildren().isEmpty()) {
      throw new IllegalStateException("List field has no children: " + field.getName());
    }
    Field itemField = field.getChildren().get(0);

    // In V2.1 the list does not consume a column.  The first child column
    // (or descendant leaf column) contains the rep/def levels.
    ColumnMetadata cm = footer.getColumnMetadatas().get(nextColIndex[0]);
    Field innermostField = getInnermostField(itemField);


    boolean hasParentRepDef = parentRepLevels != null;

    // Accumulate rep/def levels and item vectors across all pages.
    List<short[]> allRepLevels = new ArrayList<>();
    List<short[]> allDefLevels = new ArrayList<>();
    List<FieldVector> itemVectors = new ArrayList<>();
    boolean allPagesConstantLayout = true;
    for (ColumnMetadata.Page page : cm.getPagesList()) {
      PageLayout layout = PageDecoder.unpackPageLayout(page.getEncoding());
      if (layout == null || !layout.hasConstantLayout()) {
        allPagesConstantLayout = false;
        break;
      }
    }


    if (hasParentRepDef) {
      // Use parent's already-truncated rep/def instead of re-reading from file.
      allRepLevels.add(parentRepLevels.clone());
      if (parentDefLevels != null) {
        allDefLevels.add(parentDefLevels.clone());
      }
    }

    for (ColumnMetadata.Page page : cm.getPagesList()) {
      int pageRows = (int) page.getLength();

      // Read page buffers
      List<byte[]> buffers = new ArrayList<>();
      for (int i = 0; i < page.getBufferOffsetsCount(); i++) {
        long offset = page.getBufferOffsets(i);
        long size = page.getBufferSizes(i);
        ByteBuffer buf = BufferReader.readBuffer(channel, offset, size);
        byte[] data = new byte[(int) size];
        buf.get(data);
        buffers.add(data);
      }
      // page buffers read
      PageBufferStore store = new PageBufferStore(buffers);
      PageLayout layout = PageDecoder.unpackPageLayout(page.getEncoding());
      if (layout == null) {
        throw new IllegalStateException("Expected PageLayout for V2.1 list column");
      }

      short[] repLevels = null;
      short[] defLevels = null;
      int itemCount = 0;
      List<RepDefLayer> pageLayers = null;
      FieldVector itemVec = null;

      if (layout.hasMiniBlockLayout()) {
        var miniBlock = layout.getMiniBlockLayout();

        if (!hasParentRepDef) {
          repLevels = MiniBlockLayoutDecoder.extractRepetitionLevels(
              layout, pageRows, store, allocator);
          defLevels = MiniBlockLayoutDecoder.extractDefinitionLevels(
              layout, pageRows, store, allocator);
          allRepLevels.add(repLevels);
          allDefLevels.add(defLevels);
        }
        pageLayers = miniBlock.getLayersList();

        short[] currentRep = hasParentRepDef ? allRepLevels.get(0) : repLevels;
        short[] currentDef = hasParentRepDef
            ? (allDefLevels.isEmpty() ? null : allDefLevels.get(0)) : defLevels;
        boolean pageHasDef = currentDef != null && currentDef.length > 0;
        if (pageHasDef) {
          int maxVisibleLevel = computeMaxVisibleLevel(pageLayers);
          itemCount = 0;
          for (int i = 0; i < currentRep.length; i++) {
            if (currentDef[i] <= maxVisibleLevel) {
              itemCount++;
            }
          }
        } else {
          itemCount = currentRep != null ? currentRep.length : 0;
        }

        store.resetBufferIndex();
        MiniBlockLayoutDecoder decoder = new MiniBlockLayoutDecoder();
        itemVec = decoder.decode(layout, itemCount, store, innermostField, allocator);
      } else if (layout.hasConstantLayout()) {
        var constantLayout = layout.getConstantLayout();
        pageLayers = constantLayout.getLayersList();

        boolean hasListLayer = pageLayers.stream().anyMatch(l -> isListLayer(l));
        if (hasListLayer) {
          if (!hasParentRepDef) {
            if (store.getCurrentBufferIndex() < store.getBufferCount()) {
              byte[] repBuffer = store.takeNextBuffer();
              int numRep = repBuffer.length / 2;
              repLevels = new short[numRep];
              ByteBuffer repBB = ByteBuffer.wrap(repBuffer).order(ByteOrder.LITTLE_ENDIAN);
              repBB.asShortBuffer().get(repLevels);
            }
            if (store.getCurrentBufferIndex() < store.getBufferCount()) {
              byte[] defBuffer = store.takeNextBuffer();
              int numDef = defBuffer.length / 2;
              defLevels = new short[numDef];
              ByteBuffer defBB = ByteBuffer.wrap(defBuffer).order(ByteOrder.LITTLE_ENDIAN);
              defBB.asShortBuffer().get(defLevels);
            }
            allRepLevels.add(repLevels);
            allDefLevels.add(defLevels);
          }

          short[] currentRep = hasParentRepDef ? allRepLevels.get(0) : repLevels;
          short[] currentDef = hasParentRepDef
              ? (allDefLevels.isEmpty() ? null : allDefLevels.get(0)) : defLevels;
          boolean pageHasDef = currentDef != null && currentDef.length > 0;
          if (pageHasDef) {
            int maxVisibleLevel = computeMaxVisibleLevel(pageLayers);
            itemCount = 0;
            for (int i = 0; i < currentRep.length; i++) {
              if (currentDef[i] <= maxVisibleLevel) {
                itemCount++;
              }
            }
          } else {
            itemCount = currentRep != null ? currentRep.length : 0;
          }
        } else {
          if (!hasParentRepDef) {
            itemCount = repLevels != null ? repLevels.length : 0;
          }
        }

        ConstantLayoutDecoder decoder = new ConstantLayoutDecoder();
        itemVec = decoder.decode(layout, itemCount, store, innermostField, allocator);
      } else {
        throw new UnsupportedOperationException(
            "Unsupported V2.1 list layout: " + layout.getLayoutCase());
      }

      if (itemVec != null) {
        itemVectors.add(itemVec);
      }
    }

    // Merge multi-page data
    short[] repLevels = mergeShortArrays(allRepLevels);
    short[] defLevels = mergeShortArrays(allDefLevels);

    List<RepDefLayer> layers;
    if (hasParentRepDef) {
      layers = parentLayers;
    } else if (cm.getPages(0).getEncoding().hasDirect()) {
      PageLayout firstLayout = PageDecoder.unpackPageLayout(cm.getPages(0).getEncoding());
      if (firstLayout.hasMiniBlockLayout()) {
        layers = firstLayout.getMiniBlockLayout().getLayersList();
      } else if (firstLayout.hasConstantLayout()) {
        layers = firstLayout.getConstantLayout().getLayersList();
      } else {
        layers = java.util.Collections.emptyList();
      }
    } else {
      layers = java.util.Collections.emptyList();
    }

    int listLayerCount = 0;
    for (RepDefLayer layer : layers) {
      if (isListLayer(layer)) {
        listLayerCount++;
      }
    }

    boolean itemIsList = itemField.getType() instanceof ArrowType.List
        || itemField.getType() instanceof ArrowType.LargeList;

    if (listLayerCount > 1 || itemField.getType() instanceof ArrowType.Struct || itemIsList) {
      int skipLayers = countListLayersDeep(itemField);
      int unravelCount = countListLayersDeep(field) - countListLayersDeep(itemField);

      // For nested list items (non-struct), unravel all list layers here.
      if (itemIsList && !allPagesConstantLayout) {
        unravelCount = countListLayersDeep(field);
        skipLayers = 0;
      }

      // For struct items, unravel all list layers in this call and pass the
      // layer results down via DecodedArray.listResults.
      if (itemField.getType() instanceof ArrowType.Struct) {
        unravelCount = countListLayersDeep(field);
        skipLayers = 0;
      }


      RepDefUnraveler unraveler;
      if (hasParentRepDef) {
        // Parent rep/def has already been truncated by outer layers.
        // Start unraveling from the current list layer directly.
        int[] state = computeUnravelerStartState(layers, skipLayers);
        unraveler = new RepDefUnraveler(
            repLevels, defLevels, layers, state[0], state[1], state[2]);
      } else {
        unraveler = new RepDefUnraveler(repLevels, defLevels, layers);
        if (skipLayers > 0) {
          unraveler.skipListLayers(skipLayers);
        }
      }

      // Multi-layer list unravel
      java.util.List<RepDefUnraveler.UnravelResult> layerResults = new java.util.ArrayList<>();
      for (int i = 0; i < unravelCount && unraveler.hasMoreListLayers(); i++) {
        RepDefUnraveler.UnravelResult result = unraveler.unravelOffsets(numRows);

        layerResults.add(result);
      }

      // For ConstantLayout nested lists, manually build any missing inner list layer(s)
      // because RepDefUnraveler may only see the outer list layer(s).
      if (itemIsList && allPagesConstantLayout && !layerResults.isEmpty()) {
        int missingLayers = countListLayersDeep(field) - layerResults.size();
        for (int m = 0; m < missingLayers; m++) {
          int innerCount = (m == 0)
              ? layerResults.get(0).offsets[layerResults.get(0).offsets.length - 1]
              : 0;
          int[] innerOffsets = new int[innerCount + 1];
          boolean[] innerValidity = new boolean[innerCount];
          layerResults.add(0, new RepDefUnraveler.UnravelResult(innerOffsets, innerValidity));
        }
      }

      FieldVector currentVec;
      if (itemField.getType() instanceof ArrowType.Struct) {
        // Mixed list/struct nesting: build the entire tree inside-out using the
        // pre-computed layer results.  layerResults is inner-to-outer; reverse it
        // so that buildMixedNestedVector can walk from the outside in.
        // The first descendant leaf was already decoded into itemVectors.
        nextColIndex[0]++;

        java.util.List<RepDefUnraveler.UnravelResult> outerToInner =
            new java.util.ArrayList<>(layerResults);
        java.util.Collections.reverse(outerToInner);
        currentVec = buildMixedNestedVector(
            field, 0, outerToInner, itemVectors, innermostField,
            nextColIndex, footer, allocator, numRows);
        return new DecodedArray(currentVec, unraveler.getRepLevels(), unraveler.getDefLevels(), layers);
      } else {
        nextColIndex[0]++;
        currentVec = mergeFieldVectors(itemVectors, itemField, allocator);
      }

      for (int i = 0; i < layerResults.size(); i++) {
        int depthFromInner = layerResults.size() - 1 - i;
        Field currentField = getFieldAtDepth(field, depthFromInner);
        currentVec = buildListVector(currentVec, currentField, layerResults.get(i), allocator);
      }
      return new DecodedArray(currentVec, unraveler.getRepLevels(), unraveler.getDefLevels(), layers);
    }

    // Single-layer list with primitive item (unified via RepDefUnraveler)
    nextColIndex[0]++;
    FieldVector itemVec = mergeFieldVectors(itemVectors, innermostField, allocator);

    RepDefUnraveler unraveler = new RepDefUnraveler(repLevels, defLevels, layers);
    RepDefUnraveler.UnravelResult result = unraveler.unravelOffsets(numRows);

    FieldVector listVec = buildListVector(itemVec, field, result, allocator);
    // Return the original (un-truncated) rep/def levels so that upstream
    // struct validity derivation (buildStructVector) sees the full buffers.
    return new DecodedArray(listVec, repLevels, defLevels, layers);
  }

  /**
   * Recursively builds a nested list/struct tree from the inside out using pre-computed
   * list layer results.  {@code layerResults} must be in <strong>outer-to-inner</strong> order
   * (the opposite of {@code RepDefUnraveler}'s natural order).
   */
  private FieldVector buildMixedNestedVector(
      Field field,
      int layerIndex,
      java.util.List<RepDefUnraveler.UnravelResult> layerResults,
      java.util.List<FieldVector> itemVectors,
      Field innermostField,
      int[] nextColIndex,
      LanceFileFooter footer,
      BufferAllocator allocator,
      int numRows)
      throws IOException {
    if (field.getType() instanceof ArrowType.List || field.getType() instanceof ArrowType.LargeList) {
      if (layerIndex >= layerResults.size()) {
        throw new IllegalStateException("Not enough layer results for field " + field.getName());
      }
      RepDefUnraveler.UnravelResult result = layerResults.get(layerIndex);
      Field itemField = field.getChildren().get(0);
      int childNumRows = result.offsets[result.offsets.length - 1];
      FieldVector itemVec = buildMixedNestedVector(
          itemField, layerIndex + 1, layerResults, itemVectors, innermostField,
          nextColIndex, footer, allocator, childNumRows);
      return buildListVector(itemVec, field, result, allocator);
    }

    if (field.getType() instanceof ArrowType.Struct) {
      java.util.List<FieldVector> childVecs = new java.util.ArrayList<>();
      int structNumRows = -1;
      for (Field childField : field.getChildren()) {
        FieldVector childVec = buildMixedNestedVector(
            childField, layerIndex, layerResults, itemVectors, innermostField,
            nextColIndex, footer, allocator, numRows);
        if (structNumRows == -1) {
          structNumRows = childVec.getValueCount();
        }
        childVecs.add(childVec);
      }
      return buildStructWithChildren(field, childVecs, structNumRows, allocator);
    }

    // Primitive leaf
    if (!itemVectors.isEmpty() && field.equals(innermostField)) {
      return mergeFieldVectors(itemVectors, field, allocator);
    }
    ColumnMetadata cm = footer.getColumnMetadatas().get(nextColIndex[0]);
    nextColIndex[0]++;
    return decodePrimitiveColumn(cm, field, footer, numRows, allocator).vector;
  }

  private static StructVector buildStructWithChildren(
      Field structField,
      java.util.List<FieldVector> childVecs,
      int numRows,
      BufferAllocator allocator) {
    StructVector struct = (StructVector) structField.createVector(allocator);
    struct.setInitialCapacity(numRows);
    struct.allocateNew();
    for (int i = 0; i < numRows; i++) {
      org.apache.arrow.vector.BitVectorHelper.setValidityBit(
          struct.getValidityBuffer(), i, 1);
    }
    java.util.List<Field> children = structField.getChildren();
    for (int i = 0; i < children.size(); i++) {
      Field field = children.get(i);
      FieldVector vec = childVecs.get(i);
      @SuppressWarnings("unchecked")
      FieldVector slot = struct.addOrGet(
          field.getName(), field.getFieldType(),
          (Class<? extends FieldVector>) vec.getClass());
      vec.makeTransferPair(slot).transfer();
      vec.close();
    }
    struct.setValueCount(numRows);
    return struct;
  }

  /**
   * Computes the initial state (currentLayer, currentDefCmp, currentRepCmp) for a
   * RepDefUnraveler that should start at the list layer after skipping {@code skipLayers}
   * inner list layers.
   */
  private static int[] computeUnravelerStartState(List<RepDefLayer> layers, int skipLayers) {
    int currentLayer = 0;
    int currentDefCmp = 0;
    int currentRepCmp = 0;
    int skipped = 0;
    for (RepDefLayer layer : layers) {
      if (isListLayer(layer)) {
        if (skipped < skipLayers) {
          skipped++;
          currentDefCmp += defLevelsForLayer(layer);
          currentLayer++;
          continue;
        }
        break;
      }
      currentDefCmp += defLevelsForLayer(layer);
      currentLayer++;
    }
    // currentRepCmp stays 0 because parent rep/def has already had rep levels
    // decremented by outer unravel calls.
    return new int[] {currentLayer, currentDefCmp, currentRepCmp};
  }

  private static int defLevelsForLayer(RepDefLayer layer) {
    switch (layer) {
      case REPDEF_ALL_VALID_ITEM:
      case REPDEF_ALL_VALID_LIST:
        return 0;
      case REPDEF_NULLABLE_ITEM:
      case REPDEF_NULLABLE_LIST:
      case REPDEF_EMPTYABLE_LIST:
        return 1;
      case REPDEF_NULL_AND_EMPTY_LIST:
        return 2;
      default:
        return 0;
    }
  }

  /**
   * Gets the innermost non-list field from a nested list field.
   */
  private static Field getInnermostField(Field field) {
    Field current = field;
    while (!current.getChildren().isEmpty()) {
      ArrowType type = current.getType();
      if (type instanceof ArrowType.List || type instanceof ArrowType.LargeList) {
        current = current.getChildren().get(0);
      } else if (type instanceof ArrowType.Struct) {
        current = current.getChildren().get(0);
      } else {
        break;
      }
    }
    return current;
  }

  /**
   * Counts the total number of list layers in a field and all its descendants.
   * For a {@code list<struct<items: list<int>, name: string>>} this returns 2
   * (outer list + inner list).
   */
  private static int countListLayersDeep(Field field) {
    int count = 0;
    Field current = field;
    while (!current.getChildren().isEmpty()) {
      ArrowType type = current.getType();
      if (type instanceof ArrowType.List || type instanceof ArrowType.LargeList) {
        count++;
        current = current.getChildren().get(0);
      } else if (type instanceof ArrowType.Struct) {
        int maxChild = 0;
        for (Field child : current.getChildren()) {
          maxChild = Math.max(maxChild, countListLayersDeep(child));
        }
        count += maxChild;
        break;
      } else {
        break;
      }
    }
    return count;
  }

  /**
   * Recursively builds a nested list vector using a RepDefUnraveler.
   *
   * <p>This replaces the previous hard-coded 2-layer implementation with a general
   * recursive approach that follows the Rust {@code lance-encoding} design.
   */
  /**
   * Returns the field at the given depth from the outermost list layer.
   *
   * <p>For {@code list<list<list<int>>>}:
   * <ul>
   *   <li>depth 0 → {@code list<list<list<int>>>}</li>
   *   <li>depth 1 → {@code list<list<int>>}</li>
   *   <li>depth 2 → {@code list<int>}</li>
   * </ul>
   */
  private static Field getFieldAtDepth(Field rootField, int depth) {
    Field current = rootField;
    for (int i = 0; i < depth; i++) {
      if (current.getChildren().isEmpty()) {
        break;
      }
      ArrowType type = current.getType();
      if (type instanceof ArrowType.List || type instanceof ArrowType.LargeList) {
        current = current.getChildren().get(0);
      } else {
        break;
      }
    }
    return current;
  }

  /**
   * Builds a single list layer from a pre-computed {@link RepDefUnraveler.UnravelResult}.
   *
   * @param childVec the already-built child vector (inner list or leaf values)
   * @param field    the Arrow field for this list layer
   * @param result   offsets and validity for this layer
   * @param allocator Arrow buffer allocator
   * @return the built ListVector or LargeListVector
   */
  private static FieldVector buildListVector(
      FieldVector childVec,
      Field field,
      RepDefUnraveler.UnravelResult result,
      BufferAllocator allocator) {
    int numRows = result.numLists;
    // buildListVector debug trace removed
    FieldVector vector = field.createVector(allocator);
    if (vector instanceof ListVector) {
      ListVector listVec = (ListVector) vector;
      listVec.setInitialCapacity(numRows);
      listVec.allocateNew();

      org.apache.arrow.memory.ArrowBuf offsetBuf = listVec.getOffsetBuffer();
      for (int i = 0; i < result.offsets.length; i++) {
        offsetBuf.setInt(i * 4, result.offsets[i]);
      }

      for (int i = 0; i < numRows; i++) {
        if (i < result.validity.length && result.validity[i]) {
          listVec.setNotNull(i);
        } else {
          listVec.setNull(i);
        }
      }

      Field childField = field.getChildren().get(0);
      org.apache.arrow.vector.ValueVector dataVec =
          listVec.addOrGetVector(childField.getFieldType()).getVector();
      childVec.makeTransferPair(dataVec).transfer();
      childVec.close();

      listVec.setLastSet(numRows - 1);
      listVec.setValueCount(numRows);
      return listVec;
    }

    if (vector instanceof LargeListVector) {
      LargeListVector listVec = (LargeListVector) vector;
      listVec.setInitialCapacity(numRows);
      listVec.allocateNew();

      org.apache.arrow.memory.ArrowBuf offsetBuf = listVec.getOffsetBuffer();
      for (int i = 0; i < result.offsets.length; i++) {
        offsetBuf.setLong(i * 8, result.offsets[i]);
      }

      for (int i = 0; i < numRows; i++) {
        if (i < result.validity.length && result.validity[i]) {
          listVec.setNotNull(i);
        } else {
          listVec.setNull(i);
        }
      }

      Field childField = field.getChildren().get(0);
      org.apache.arrow.vector.ValueVector dataVec =
          listVec.addOrGetVector(childField.getFieldType()).getVector();
      childVec.makeTransferPair(dataVec).transfer();
      childVec.close();

      listVec.setLastSet(numRows - 1);
      listVec.setValueCount(numRows);
      return listVec;
    }

    throw new UnsupportedOperationException(
        "Unsupported list vector type: " + vector.getClass().getName());
  }

  private static FieldVector buildRecursiveListVector(
      RepDefUnraveler unraveler,
      FieldVector innerVec,
      Field field,
      int numRows,
      BufferAllocator allocator) {
    StructuralListDecodeTask task =
        new StructuralListDecodeTask(unraveler, innerVec, field, numRows, allocator);
    return task.decode();
  }

  private static short[] mergeShortArrays(List<short[]> arrays) {
    int totalLen = 0;
    for (short[] arr : arrays) {
      if (arr != null) {
        totalLen += arr.length;
      }
    }
    short[] result = new short[totalLen];
    int pos = 0;
    for (short[] arr : arrays) {
      if (arr != null) {
        System.arraycopy(arr, 0, result, pos, arr.length);
        pos += arr.length;
      }
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  private static FieldVector mergeFieldVectors(
      List<FieldVector> vectors, Field field, BufferAllocator allocator) {
    if (vectors.size() == 1) {
      return vectors.get(0);
    }
    FieldVector target = (FieldVector) field.createVector(allocator);
    target.allocateNew();
    FieldVector[] sources = vectors.toArray(new FieldVector[0]);
    org.apache.arrow.vector.util.VectorBatchAppender.batchAppend(target, sources);
    for (FieldVector v : vectors) {
      v.close();
    }
    target.setValueCount(target.getValueCount());
    return target;
  }

  private static int computeMaxVisibleLevel(List<RepDefLayer> layers) {
    int level = 0;
    for (RepDefLayer layer : layers) {
      if (isListLayer(layer)) {
        break;
      }
      switch (layer) {
        case REPDEF_ALL_VALID_ITEM:
        case REPDEF_ALL_VALID_LIST:
          break;
        case REPDEF_NULLABLE_ITEM:
        case REPDEF_NULLABLE_LIST:
        case REPDEF_EMPTYABLE_LIST:
          level += 1;
          break;
        case REPDEF_NULL_AND_EMPTY_LIST:
          level += 2;
          break;
        default:
          break;
      }
    }
    return level;
  }

  /**
   * Returns the def level that indicates a null <strong>innermost</strong> item.
   * Scans layers inner-to-outer and stops at the first {@code NullableItem}.
   */
  private static int computeNullItemLevel(List<RepDefLayer> layers) {
    int currentDef = 0;
    for (RepDefLayer layer : layers) {
      switch (layer) {
        case REPDEF_ALL_VALID_ITEM:
        case REPDEF_ALL_VALID_LIST:
          break;
        case REPDEF_NULLABLE_ITEM:
          return currentDef + 1;
        case REPDEF_NULLABLE_LIST:
        case REPDEF_EMPTYABLE_LIST:
          currentDef += 1;
          break;
        case REPDEF_NULL_AND_EMPTY_LIST:
          currentDef += 2;
          break;
        default:
          break;
      }
    }
    return -1;
  }

  /**
   * Returns the def level that indicates a null <strong>outermost</strong> item
   * (e.g. a nullable struct). Scans layers inner-to-outer and returns the
   * <em>last</em> {@code NullableItem} encountered before the first list layer.
   */
  private static int computeOuterNullItemLevel(List<RepDefLayer> layers) {
    int currentDef = 0;
    int outerNullItemLevel = -1;
    for (RepDefLayer layer : layers) {
      switch (layer) {
        case REPDEF_ALL_VALID_ITEM:
        case REPDEF_ALL_VALID_LIST:
          break;
        case REPDEF_NULLABLE_ITEM:
          outerNullItemLevel = currentDef + 1;
          currentDef += 1;
          break;
        case REPDEF_NULLABLE_LIST:
        case REPDEF_EMPTYABLE_LIST:
          currentDef += 1;
          break;
        case REPDEF_NULL_AND_EMPTY_LIST:
          currentDef += 2;
          break;
        default:
          break;
      }
    }
    return outerNullItemLevel;
  }

  private static int computeNullListLevel(List<RepDefLayer> layers) {
    int currentDef = 0;
    for (RepDefLayer layer : layers) {
      switch (layer) {
        case REPDEF_ALL_VALID_ITEM:
        case REPDEF_ALL_VALID_LIST:
          break;
        case REPDEF_NULLABLE_ITEM:
          currentDef += 1;
          break;
        case REPDEF_NULLABLE_LIST:
          return currentDef + 1;
        case REPDEF_EMPTYABLE_LIST:
          currentDef += 1;
          break;
        case REPDEF_NULL_AND_EMPTY_LIST:
          return currentDef + 1;
        default:
          break;
      }
    }
    return -1;
  }

  private static int computeEmptyListLevel(List<RepDefLayer> layers) {
    int currentDef = 0;
    for (RepDefLayer layer : layers) {
      switch (layer) {
        case REPDEF_ALL_VALID_ITEM:
        case REPDEF_ALL_VALID_LIST:
          break;
        case REPDEF_NULLABLE_ITEM:
          currentDef += 1;
          break;
        case REPDEF_NULLABLE_LIST:
          currentDef += 1;
          break;
        case REPDEF_EMPTYABLE_LIST:
          return currentDef + 1;
        case REPDEF_NULL_AND_EMPTY_LIST:
          return currentDef + 2;
        default:
          break;
      }
    }
    return -1;
  }

  private static boolean isListLayer(RepDefLayer layer) {
    return layer == lance.encodings21.EncodingsV21.RepDefLayer.REPDEF_ALL_VALID_LIST
        || layer == lance.encodings21.EncodingsV21.RepDefLayer.REPDEF_NULLABLE_LIST
        || layer == lance.encodings21.EncodingsV21.RepDefLayer.REPDEF_EMPTYABLE_LIST
        || layer == lance.encodings21.EncodingsV21.RepDefLayer.REPDEF_NULL_AND_EMPTY_LIST;
  }

  /**
   * Checks whether an Arrow field is marked as a packed struct via metadata.
   */
  private static boolean isPackedStructField(Field field) {
    Map<String, String> metadata = field.getMetadata();
    return metadata != null && "true".equals(metadata.get("packed"));
  }

  /**
   * Checks whether an Arrow field is marked as a blob via metadata.
   */
  private static boolean isBlobField(Field field) {
    Map<String, String> metadata = field.getMetadata();
    return metadata != null && "true".equals(metadata.get("lance-encoding:blob"));
  }

  /**
   * Checks whether a column uses PackedStruct encoding (V2.0).
   *
   * <p>PackedStruct stores all child fields in a single buffer, consuming only one column
   * index. This is the opposite of SimpleStruct where each child gets its own column.
   */
  private static boolean isPackedStructColumn(ColumnMetadata cm, boolean isV21) {
    if (isV21 || cm.getPagesCount() == 0) {
      return false;
    }
    ColumnMetadata.Page page = cm.getPages(0);
    lance.file.v2.File2.Encoding enc = page.getEncoding();
    lance.encodings.EncodingsV20.ArrayEncoding arrayEnc =
        PageDecoder.unpackArrayEncoding(enc);
    if (arrayEnc == null) {
      return false;
    }
    return containsPackedStruct(arrayEnc);
  }

  private static boolean containsPackedStruct(lance.encodings.EncodingsV20.ArrayEncoding encoding) {
    if (encoding.hasPackedStruct()) {
      return true;
    }
    if (encoding.hasNullable()) {
      var nullable = encoding.getNullable();
      if (nullable.hasNoNulls()) {
        return containsPackedStruct(nullable.getNoNulls().getValues());
      }
      if (nullable.hasAllNulls()) {
        return false;
      }
      if (nullable.hasSomeNulls()) {
        return containsPackedStruct(nullable.getSomeNulls().getValues());
      }
    }
    return false;
  }

  @Override
  public void close() throws IOException {
    channel.close();
  }
}
