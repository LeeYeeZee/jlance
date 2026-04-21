package com.github.jlance.compat;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.jlance.format.LanceFileMetadata;
import com.github.jlance.format.LanceFileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

public class Milestone02CompatTest {

  @Test
  public void testReadSchemaAndRowCount() throws Exception {
    Path base = Paths.get("..", "compat_tests", "data", "milestone_02", "test.lance", "data");
    Path[] files;
    try (var stream = java.nio.file.Files.list(base)) {
      files = stream.filter(p -> p.toString().endsWith(".lance")).toArray(Path[]::new);
    }
    assertThat(files).hasSize(1);
    Path lanceFile = files[0];

    Schema expectedSchema =
        new Schema(
            Arrays.asList(
                new Field("int8_col", FieldType.notNullable(new ArrowType.Int(8, true)), null),
                new Field("int16_col", FieldType.nullable(new ArrowType.Int(16, true)), null),
                new Field("int32_col", FieldType.notNullable(new ArrowType.Int(32, true)), null),
                new Field("int64_col", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("uint8_col", FieldType.notNullable(new ArrowType.Int(8, false)), null),
                new Field(
                    "uint16_col", FieldType.nullable(new ArrowType.Int(16, false)), null),
                new Field(
                    "uint32_col", FieldType.notNullable(new ArrowType.Int(32, false)), null),
                new Field("uint64_col", FieldType.nullable(new ArrowType.Int(64, false)), null),
                new Field(
                    "float_col",
                    FieldType.notNullable(
                        new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
                    null),
                new Field(
                    "double_col",
                    FieldType.nullable(
                        new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                    null),
                new Field("bool_col", FieldType.notNullable(ArrowType.Bool.INSTANCE), null),
                new Field("string_col", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
                new Field("binary_col", FieldType.nullable(ArrowType.Binary.INSTANCE), null)));

    try (LanceFileReader reader = new LanceFileReader(lanceFile)) {
      LanceFileMetadata metadata = reader.readMetadata();
      assertThat(metadata.getSchema()).isEqualTo(expectedSchema);
      assertThat(metadata.getNumRows()).isEqualTo(3L);
    }
  }
}
