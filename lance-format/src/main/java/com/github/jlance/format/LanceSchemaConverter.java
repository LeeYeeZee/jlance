package com.github.jlance.format;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lance.file.File.Field;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

/**
 * Converts Lance protobuf {@link lance.file.Field} definitions into Apache Arrow {@link
 * org.apache.arrow.vector.types.pojo.Field} objects.
 */
public class LanceSchemaConverter {

  private LanceSchemaConverter() {}

  /**
   * Converts a list of Lance fields (in depth-first order) into an Arrow {@link
   * org.apache.arrow.vector.types.pojo.Schema}.
   */
  public static List<org.apache.arrow.vector.types.pojo.Field> convertFields(
      List<Field> lanceFields) {
    if (lanceFields.isEmpty()) {
      return Collections.emptyList();
    }

    // Build parent-child relationships
    Map<Integer, List<Field>> childrenByParent = new HashMap<>();
    for (Field f : lanceFields) {
      childrenByParent.computeIfAbsent(f.getParentId(), k -> new ArrayList<>()).add(f);
    }

    // Top-level fields have parent_id == -1 (or 0 if using older files)
    List<Field> topLevel = childrenByParent.getOrDefault(-1, Collections.emptyList());
    if (topLevel.isEmpty()) {
      topLevel = childrenByParent.getOrDefault(0, Collections.emptyList());
    }
    List<org.apache.arrow.vector.types.pojo.Field> arrowFields = new ArrayList<>();
    for (Field f : topLevel) {
      arrowFields.add(convertField(f, childrenByParent));
    }
    return arrowFields;
  }

  private static org.apache.arrow.vector.types.pojo.Field convertField(
      Field lanceField, Map<Integer, List<Field>> childrenByParent) {
    String name = lanceField.getName();
    boolean nullable = lanceField.getNullable();
    String logicalType = lanceField.getLogicalType();
    ArrowType arrowType = parseLogicalType(logicalType);

    List<org.apache.arrow.vector.types.pojo.Field> children = new ArrayList<>();
    if (logicalType.startsWith("fixed_size_list:")) {
      String[] parts = logicalType.split(":");
      if (parts.length == 3) {
        ArrowType itemType = parseLogicalType(parts[1]);
        children.add(
            new org.apache.arrow.vector.types.pojo.Field(
                "item", FieldType.nullable(itemType), null));
      }
    } else {
      for (Field child :
          childrenByParent.getOrDefault(lanceField.getId(), Collections.emptyList())) {
        children.add(convertField(child, childrenByParent));
      }
    }

    FieldType fieldType = new FieldType(nullable, arrowType, null, null);
    return new org.apache.arrow.vector.types.pojo.Field(name, fieldType, children);
  }

  static ArrowType parseLogicalType(String logicalType) {
    if (logicalType == null || logicalType.isEmpty()) {
      throw new UnsupportedOperationException("Empty logical type");
    }

    return switch (logicalType) {
      case "null" -> ArrowType.Null.INSTANCE;
      case "bool" -> ArrowType.Bool.INSTANCE;
      case "int8" -> new ArrowType.Int(8, true);
      case "uint8" -> new ArrowType.Int(8, false);
      case "int16" -> new ArrowType.Int(16, true);
      case "uint16" -> new ArrowType.Int(16, false);
      case "int32" -> new ArrowType.Int(32, true);
      case "uint32" -> new ArrowType.Int(32, false);
      case "int64" -> new ArrowType.Int(64, true);
      case "uint64" -> new ArrowType.Int(64, false);
      case "halffloat" -> new ArrowType.FloatingPoint(FloatingPointPrecision.HALF);
      case "float" -> new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
      case "double" -> new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
      case "string" -> ArrowType.Utf8.INSTANCE;
      case "large_string" -> ArrowType.LargeUtf8.INSTANCE;
      case "binary" -> ArrowType.Binary.INSTANCE;
      case "large_binary" -> ArrowType.LargeBinary.INSTANCE;
      case "date32:day" -> new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY);
      case "date64:ms" -> new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.MILLISECOND);
      case "time32:s" -> new ArrowType.Time(org.apache.arrow.vector.types.TimeUnit.SECOND, 32);
      case "time32:ms" -> new ArrowType.Time(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, 32);
      case "time64:us" -> new ArrowType.Time(org.apache.arrow.vector.types.TimeUnit.MICROSECOND, 64);
      case "time64:ns" -> new ArrowType.Time(org.apache.arrow.vector.types.TimeUnit.NANOSECOND, 64);
      default -> {
        if (logicalType.startsWith("decimal:")) {
          yield parseDecimal(logicalType);
        } else if (logicalType.startsWith("timestamp:")) {
          yield parseTimestamp(logicalType);
        } else if (logicalType.startsWith("time:")) {
          yield parseTime(logicalType);
        } else if (logicalType.startsWith("duration:")) {
          yield parseDuration(logicalType);
        } else if (logicalType.startsWith("dict:")) {
          // dict:value_type:index_type:false
          // value_type may itself contain colons (e.g. decimal:128:10:2)
          String[] parts = logicalType.split(":");
          if (parts.length < 4) {
            throw new IllegalArgumentException("Invalid dict type: " + logicalType);
          }
          String valueType = String.join(":", java.util.Arrays.copyOfRange(parts, 1, parts.length - 2));
          yield parseLogicalType(valueType);
        } else if (logicalType.startsWith("fixed_size_list:")) {
          String[] parts = logicalType.split(":");
          if (parts.length != 3) {
            throw new IllegalArgumentException("Invalid fixed_size_list type: " + logicalType);
          }
          yield new ArrowType.FixedSizeList(Integer.parseInt(parts[2]));
        } else if (logicalType.startsWith("fixed_size_binary:")) {
          String[] parts = logicalType.split(":");
          if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid fixed_size_binary type: " + logicalType);
          }
          yield new ArrowType.FixedSizeBinary(Integer.parseInt(parts[1]));
        } else if (logicalType.equals("struct")) {
          yield ArrowType.Struct.INSTANCE;
        } else if (logicalType.equals("list") || logicalType.equals("list.struct")) {
          yield ArrowType.List.INSTANCE;
        } else if (logicalType.equals("large_list") || logicalType.equals("large_list.struct")) {
          yield ArrowType.LargeList.INSTANCE;
        } else {
          throw new UnsupportedOperationException("Unsupported logical type: " + logicalType);
        }
      }
    };
  }

  private static ArrowType parseDecimal(String logicalType) {
    // decimal:128:precision:scale or decimal:256:precision:scale
    String[] parts = logicalType.split(":");
    if (parts.length != 4) {
      throw new IllegalArgumentException("Invalid decimal type: " + logicalType);
    }
    int bitWidth = Integer.parseInt(parts[1]);
    int precision = Integer.parseInt(parts[2]);
    int scale = Integer.parseInt(parts[3]);
    return new ArrowType.Decimal(precision, scale, bitWidth);
  }

  private static ArrowType parseTimestamp(String logicalType) {
    // timestamp:unit or timestamp:unit:timezone
    String rest = logicalType.substring("timestamp:".length());
    String unit;
    String timezone = null;
    int colonIdx = rest.indexOf(':');
    if (colonIdx >= 0) {
      unit = rest.substring(0, colonIdx);
      timezone = rest.substring(colonIdx + 1);
      if (timezone.isEmpty() || "-".equals(timezone)) {
        timezone = null;
      }
    } else {
      unit = rest;
    }
    org.apache.arrow.vector.types.TimeUnit arrowUnit = parseTimeUnit(unit);
    return new ArrowType.Timestamp(arrowUnit, timezone);
  }

  private static ArrowType parseTime(String logicalType) {
    // time:unit or time:unit:timezone
    String rest = logicalType.substring("time:".length());
    String unit;
    int colonIdx = rest.indexOf(':');
    if (colonIdx >= 0) {
      unit = rest.substring(0, colonIdx);
    } else {
      unit = rest;
    }
    org.apache.arrow.vector.types.TimeUnit arrowUnit = parseTimeUnit(unit);
    int bitWidth = (arrowUnit == org.apache.arrow.vector.types.TimeUnit.SECOND
        || arrowUnit == org.apache.arrow.vector.types.TimeUnit.MILLISECOND) ? 32 : 64;
    return new ArrowType.Time(arrowUnit, bitWidth);
  }

  private static ArrowType parseDuration(String logicalType) {
    // duration:unit or duration:unit:timezone
    String rest = logicalType.substring("duration:".length());
    String unit;
    int colonIdx = rest.indexOf(':');
    if (colonIdx >= 0) {
      unit = rest.substring(0, colonIdx);
    } else {
      unit = rest;
    }
    org.apache.arrow.vector.types.TimeUnit arrowUnit = parseTimeUnit(unit);
    return new ArrowType.Duration(arrowUnit);
  }

  private static org.apache.arrow.vector.types.TimeUnit parseTimeUnit(String unit) {
    return switch (unit) {
      case "s" -> org.apache.arrow.vector.types.TimeUnit.SECOND;
      case "ms" -> org.apache.arrow.vector.types.TimeUnit.MILLISECOND;
      case "us" -> org.apache.arrow.vector.types.TimeUnit.MICROSECOND;
      case "ns" -> org.apache.arrow.vector.types.TimeUnit.NANOSECOND;
      default -> throw new IllegalArgumentException("Unknown time unit: " + unit);
    };
  }
}
