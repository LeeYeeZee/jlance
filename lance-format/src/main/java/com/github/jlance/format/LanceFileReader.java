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
      return buildStructVector(field, nextColIndex, footer, numRows, allocator);
    }

    ColumnMetadata cm = footer.getColumnMetadatas().get(nextColIndex[0]);
    nextColIndex[0]++;

    if (field.getType() instanceof ArrowType.List || field.getType() instanceof ArrowType.LargeList) {
      FieldVector vector = decodeListColumn(cm, field, nextColIndex, footer, numRows, allocator);
      return new DecodedArray(vector);
    }

    return decodePrimitiveColumn(cm, field, footer, numRows, allocator);
  }

  private DecodedArray buildStructVector(
      Field field,
      int[] nextColIndex,
      LanceFileFooter footer,
      int numRows,
      BufferAllocator allocator)
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
      DecodedArray childArray = readField(childField, nextColIndex, footer, numRows, allocator);
      childArrays.add(childArray);
      FieldVector childVec = childArray.vector;
      @SuppressWarnings("unchecked")
      FieldVector slot =
          struct.addOrGet(
              childField.getName(), childField.getFieldType(),
              (Class<? extends FieldVector>) childVec.getClass());
      childVec.makeTransferPair(slot).transfer();
      childVec.close();
    }

    // Try to get rep/def from first child's DecodedArray (V2.1+ path)
    if (isNullableStruct && !childArrays.isEmpty()) {
      DecodedArray firstChild = childArrays.get(0);
      if (firstChild.defLevels != null && firstChild.layers != null) {
        for (int li = firstChild.layers.size() - 1; li >= 0; li--) {
          if (firstChild.layers.get(li)
              == lance.encodings21.EncodingsV21.RepDefLayer.REPDEF_NULLABLE_ITEM) {
            structLayerIndex = li;
            structDefLevels = firstChild.defLevels;
            break;
          }
        }
      }
    }

    // Fallback: peek the first child's page directly if DecodedArray didn't carry rep/def
    if (structDefLevels == null && isNullableStruct && !childArrays.isEmpty()) {
      int firstChildColIndex = nextColIndex[0] - childArrays.size();
      ColumnMetadata cm = footer.getColumnMetadatas().get(firstChildColIndex);
      if (cm.getPagesCount() > 0) {
        var page = cm.getPages(0);
        List<byte[]> buffers = new ArrayList<>();
        for (int i = 0; i < page.getBufferOffsetsCount(); i++) {
          long offset = page.getBufferOffsets(i);
          long size = page.getBufferSizes(i);
          ByteBuffer buf = BufferReader.readBuffer(channel, offset, size);
          byte[] data = new byte[(int) size];
          buf.get(data);
          buffers.add(data);
        }
        PageBufferStore store = new PageBufferStore(buffers);
        var layout = PageDecoder.unpackPageLayout(page.getEncoding());
        if (layout != null) {
          java.util.List<lance.encodings21.EncodingsV21.RepDefLayer> layers = null;
          if (layout.hasMiniBlockLayout()) {
            layers = layout.getMiniBlockLayout().getLayersList();
          } else if (layout.hasConstantLayout()) {
            layers = layout.getConstantLayout().getLayersList();
          } else if (layout.hasFullZipLayout()) {
            layers = layout.getFullZipLayout().getLayersList();
          }
          if (layers != null) {
            for (int li = layers.size() - 1; li >= 0; li--) {
              if (layers.get(li)
                  == lance.encodings21.EncodingsV21.RepDefLayer.REPDEF_NULLABLE_ITEM) {
                structLayerIndex = li;
                break;
              }
            }
            if (structLayerIndex >= 0) {
              if (layout.hasMiniBlockLayout()) {
                structDefLevels =
                    com.github.jlance.format.decoder.MiniBlockLayoutDecoder
                        .extractDefinitionLevels(layout, numRows, store, allocator);
              } else if (layout.hasConstantLayout()) {
                structDefLevels =
                    com.github.jlance.format.decoder.ConstantLayoutDecoder
                        .extractDefinitionLevels(layout, numRows, store, allocator);
              } else if (layout.hasFullZipLayout()) {
                structDefLevels =
                    com.github.jlance.format.decoder.FullZipLayoutDecoder
                        .extractDefinitionLevels(layout, numRows, store, allocator);
              }
            }
          }
        }
      }
    }

    if (structDefLevels != null && structLayerIndex >= 0) {
      int mask = 1 << structLayerIndex;
      for (int i = 0; i < numRows; i++) {
        boolean structValid = (structDefLevels[i] & mask) == 0;
        org.apache.arrow.vector.BitVectorHelper.setValidityBit(
            struct.getValidityBuffer(), i, structValid ? 1 : 0);
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
    if (footer.isV2_1OrLater()) {
      PageLayout layout = PageDecoder.unpackPageLayout(cm.getPages(0).getEncoding());
      if (layout != null) {
        return decodeV21ListColumn(cm, field, numRows, allocator);
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
      try {
        for (int i = 0; i < buffers.size(); i++) {
          java.nio.file.Files.write(
              java.nio.file.Path.of("C:/Users/22591/jlance/debug_buffer_" + i + ".bin"),
              buffers.get(i));
        }
      } catch (Exception e) {
        // ignore
      }
      // page buffers consumed
      for (int i = 0; i < buffers.size(); i++) {
        // buffer size logged
      }
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
   */
  private FieldVector decodeV21ListColumn(
      ColumnMetadata cm,
      Field field,
      int numRows,
      BufferAllocator allocator)
      throws IOException {
    if (field.getChildren().isEmpty()) {
      throw new IllegalStateException("List field has no children: " + field.getName());
    }
    Field itemField = field.getChildren().get(0);
    Field innermostField = getInnermostField(itemField);

    // Accumulate rep/def levels and item vectors across all pages.
    List<short[]> allRepLevels = new ArrayList<>();
    List<short[]> allDefLevels = new ArrayList<>();
    List<FieldVector> itemVectors = new ArrayList<>();
    int totalRows = 0;

    for (ColumnMetadata.Page page : cm.getPagesList()) {
      int pageRows = (int) page.getLength();
      totalRows += pageRows;

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
      try {
        for (int i = 0; i < buffers.size(); i++) {
          java.nio.file.Files.write(
              java.nio.file.Path.of("C:/Users/22591/jlance/debug_buffer_" + i + ".bin"),
              buffers.get(i));
        }
      } catch (Exception e) {
        // ignore
      }
      PageBufferStore store = new PageBufferStore(buffers);
      PageLayout layout = PageDecoder.unpackPageLayout(page.getEncoding());
      if (layout == null) {
        throw new IllegalStateException("Expected PageLayout for V2.1 list column");
      }

      // Extract rep/def levels based on layout type
      short[] repLevels = null;
      short[] defLevels = null;
      int itemCount = 0;
      List<RepDefLayer> pageLayers;
      FieldVector itemVec;

      if (layout.hasMiniBlockLayout()) {
        var miniBlock = layout.getMiniBlockLayout();
        repLevels = MiniBlockLayoutDecoder.extractRepetitionLevels(
            layout, pageRows, store, allocator);
        defLevels = MiniBlockLayoutDecoder.extractDefinitionLevels(
            layout, pageRows, store, allocator);
        pageLayers = miniBlock.getLayersList();

        boolean pageHasDef = defLevels != null && defLevels.length > 0;
        if (pageHasDef) {
          int maxVisibleLevel = computeMaxVisibleLevel(pageLayers);
          itemCount = 0;
          for (int i = 0; i < repLevels.length; i++) {
            if (defLevels[i] <= maxVisibleLevel) {
              itemCount++;
            }
          }
        } else {
          itemCount = repLevels != null ? repLevels.length : 0;
        }

        store.resetBufferIndex();
        MiniBlockLayoutDecoder decoder = new MiniBlockLayoutDecoder();
        itemVec = decoder.decode(layout, itemCount, store, innermostField, allocator);
      } else if (layout.hasConstantLayout()) {
        var constantLayout = layout.getConstantLayout();
        pageLayers = constantLayout.getLayersList();

        boolean hasListLayer = pageLayers.stream().anyMatch(l -> isListLayer(l));
        if (hasListLayer
            && constantLayout.getNumRepValues() == 0
            && constantLayout.getNumDefValues() == 0) {
          // No rep/def values: all rows have the same structure determined by layers.
          // Consume any present buffers (they may be padding or unused).
          if (store.getCurrentBufferIndex() < store.getBufferCount()) {
            store.takeNextBuffer();
          }
          if (store.getCurrentBufferIndex() < store.getBufferCount()) {
            store.takeNextBuffer();
          }
          repLevels = new short[0];
          defLevels = new short[0];
          itemCount = 0;
        } else if (hasListLayer) {
          // Read rep/def from page buffers (raw u16 values)
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

          boolean pageHasDef = defLevels != null && defLevels.length > 0;
          if (pageHasDef) {
            int maxVisibleLevel = computeMaxVisibleLevel(pageLayers);
            itemCount = 0;
            for (int i = 0; i < repLevels.length; i++) {
              if (defLevels[i] <= maxVisibleLevel) {
                itemCount++;
              }
            }
          } else {
            itemCount = repLevels != null ? repLevels.length : 0;
          }
        } else {
          itemCount = repLevels != null ? repLevels.length : 0;
        }

        // For ConstantLayout, store buffers already consumed (rep/def)
        // No value buffer for all-null/all-empty
        ConstantLayoutDecoder decoder = new ConstantLayoutDecoder();
        itemVec = decoder.decode(layout, itemCount, store, innermostField, allocator);
      } else {
        throw new UnsupportedOperationException(
            "Unsupported V2.1 list layout: " + layout.getLayoutCase());
      }

      allRepLevels.add(repLevels);
      allDefLevels.add(defLevels);
      itemVectors.add(itemVec);
      // page itemCount decoded
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < Math.min(10, itemVec.getValueCount()); i++) {
        sb.append("[").append(i).append("]").append(itemVec.isNull(i) ? "null" : "v").append(" ");
      }
      // first 10 items null status checked
    }

    // Merge multi-page data
    short[] repLevels = mergeShortArrays(allRepLevels);
    short[] defLevels = mergeShortArrays(allDefLevels);
    // repLevels and defLevels merged successfully
    FieldVector itemVec = mergeFieldVectors(itemVectors, innermostField, allocator);

    // Compute list structure from rep/def levels
    List<RepDefLayer> layers;
    if (cm.getPages(0).getEncoding().hasDirect()) {
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

    int maxVisibleLevel = computeMaxVisibleLevel(layers);
    int nullItemLevel = computeNullItemLevel(layers);
    int nullListLevel = computeNullListLevel(layers);
    int emptyListLevel = computeEmptyListLevel(layers);
    boolean hasDef = defLevels != null && defLevels.length > 0;

    int listLayerCount = 0;
    for (RepDefLayer layer : layers) {
      if (isListLayer(layer)) {
        listLayerCount++;
      }
    }
    if (listLayerCount > 1) {
      V21ListUnraveler unraveler = new V21ListUnraveler(repLevels, defLevels, layers);

      // Pre-compute all list layers (inner-to-outer order).
      // Each call to unravelOffsets consumes one list layer.
      java.util.List<V21ListUnraveler.UnravelResult> layerResults = new java.util.ArrayList<>();
      while (unraveler.hasMoreListLayers()) {
        layerResults.add(unraveler.unravelOffsets(numRows));
      }

      // Build vectors bottom-up: start with the leaf items and wrap each layer.
      FieldVector currentVec = itemVec;
      Field currentField = getInnermostField(field);
      for (int i = 0; i < layerResults.size(); i++) {
        // Walk up from innermost field to the field for this layer.
        // layerResults[0] = innermost list, layerResults[last] = outermost list.
        int depthFromInner = layerResults.size() - 1 - i;
        currentField = getFieldAtDepth(field, depthFromInner);
        currentVec = buildListVector(currentVec, currentField, layerResults.get(i), allocator);
      }
      return currentVec;
    }

    int[] offsets = new int[numRows + 1];
    boolean[] listValidity = new boolean[numRows];
    int itemCount = itemVec.getValueCount();
    boolean[] itemValidity = new boolean[itemCount];
    java.util.Arrays.fill(itemValidity, true);

    int rowIdx = 0;
    int itemIdx = 0;

    if (repLevels != null && repLevels.length > 0) {
      for (int i = 0; i < repLevels.length; i++) {
        short rep = repLevels[i];
        short def = hasDef ? defLevels[i] : 0;

        if (rep > 0) {
          if (rowIdx > 0) {
            offsets[rowIdx] = itemIdx;
          }
          rowIdx++;
        }

        if (hasDef) {
          if (def <= maxVisibleLevel) {
            // Actual item (normal or null item)
            if (itemIdx < itemCount) {
              itemValidity[itemIdx] = (def != nullItemLevel);
            }
            itemIdx++;
          } else if (def == nullListLevel) {
            listValidity[rowIdx - 1] = false;
          } else if (def == emptyListLevel) {
            listValidity[rowIdx - 1] = true;
          }
        } else {
          // No def levels: every entry is an actual item
          itemIdx++;
        }
      }
    }
    offsets[numRows] = itemIdx;

    // Normal lists (non-null, non-empty) default to valid
    for (int i = 0; i < numRows; i++) {
      if (offsets[i + 1] > offsets[i]) {
        listValidity[i] = true;
      }
    }

    // Apply item validity
    for (int i = 0; i < itemCount; i++) {
      if (!itemValidity[i]) {
        itemVec.setNull(i);
      }
    }

    // Build Arrow ListVector or LargeListVector
    FieldVector vector = field.createVector(allocator);
    if (vector instanceof ListVector) {
      ListVector listVec = (ListVector) vector;
      listVec.setInitialCapacity(numRows);
      listVec.allocateNew();

      org.apache.arrow.memory.ArrowBuf offsetBuf = listVec.getOffsetBuffer();
      for (int i = 0; i <= numRows; i++) {
        offsetBuf.setInt(i * 4, offsets[i]);
      }
      for (int i = 0; i < numRows; i++) {
        if (listValidity[i]) {
          listVec.setNotNull(i);
        } else {
          listVec.setNull(i);
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
        offsetBuf.setLong(i * 8, offsets[i]);
      }
      for (int i = 0; i < numRows; i++) {
        if (listValidity[i]) {
          listVec.setNotNull(i);
        } else {
          listVec.setNull(i);
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
   * Gets the innermost non-list field from a nested list field.
   */
  private static Field getInnermostField(Field field) {
    Field current = field;
    while (!current.getChildren().isEmpty()) {
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
   * Recursively builds a nested list vector using a V21ListUnraveler.
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
   * Builds a single list layer from a pre-computed {@link V21ListUnraveler.UnravelResult}.
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
      V21ListUnraveler.UnravelResult result,
      BufferAllocator allocator) {
    int numRows = result.numLists;
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
      V21ListUnraveler unraveler,
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
