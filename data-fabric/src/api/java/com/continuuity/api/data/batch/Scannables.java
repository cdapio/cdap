package com.continuuity.api.data.batch;

import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.continuuity.internal.io.Schema;
import com.continuuity.internal.io.UnsupportedTypeException;

import java.io.StringWriter;
import java.lang.reflect.Type;
import java.util.List;

/**
 * Utility methods for row scanners.
 */
public class Scannables {

  /**
   * Provides a way to convert a key and a value - as provided by a split reader - in to a single row object.
   * @param <KEY> the key type
   * @param <VALUE> the value type
   * @param <ROW> the type representing a row
   */
  public interface RowMaker<KEY, VALUE, ROW> {

    /**
     * Convert a single key/value pair into a row.
     * @param key the key
     * @param value the value
     * @return the row
     */
    ROW makeRow(KEY key, VALUE value);
  }

  /**
   * Given a split reader and a way to convert its key/value pairs into rows, return a split row scanner that
   * delegates all operations to the underlying split reader.
   */
  public static <KEY, VALUE, ROW>
  SplitRowScanner<ROW> splitRowScanner(final SplitReader<KEY, VALUE> splitReader,
                                       final RowMaker<KEY, VALUE, ROW> rowMaker) {
    return new SplitRowScanner<ROW>() {
      @Override
      public void initialize(Split split) throws InterruptedException {
        splitReader.initialize(split);
      }

      @Override
      public boolean nextRow() throws InterruptedException {
        return splitReader.nextKeyValue();
      }

      @Override
      public ROW getCurrentRow() throws InterruptedException {
        return rowMaker.makeRow(splitReader.getCurrentKey(), splitReader.getCurrentValue());
      }

      @Override
      public void close() {
        splitReader.close();
      }

      @Override
      public float getProgress() throws InterruptedException {
        return splitReader.getProgress();
      }
    };
  }

  /**
   * Given a split reader and a way to convert its key/value pairs into rows, return a split row scanner that
   * delegates all operations to the underlying split reader.
   */
  public static <KEY, VALUE>
  SplitRowScanner<VALUE> valueRowScanner(final SplitReader<KEY, VALUE> splitReader) {
    return new SplitRowScanner<VALUE>() {
      @Override
      public void initialize(Split split) throws InterruptedException {
        splitReader.initialize(split);
      }

      @Override
      public boolean nextRow() throws InterruptedException {
        return splitReader.nextKeyValue();
      }

      @Override
      public VALUE getCurrentRow() throws InterruptedException {
        return splitReader.getCurrentValue();
      }

      @Override
      public void close() {
        splitReader.close();
      }

      @Override
      public float getProgress() throws InterruptedException {
        return splitReader.getProgress();
      }
    };
  }


  /**
   * Given a row-scannable dataset, determine its row type and generate a schema string compatible with Hive.
   * @param dataset The data set
   * @param <ROW> The row type
   * @return the hive schema
   * @throws UnsupportedTypeException if the row type is not a record or contains null types.
   */
  public static <ROW> String hiveSchemaFor(RowScannable<ROW> dataset) throws UnsupportedTypeException {
    return hiveSchemaFor(dataset.getRowType());
  }

  public static String hiveSchemaFor(Type type) throws UnsupportedTypeException {

    Schema schema = new ReflectionSchemaGenerator().generate(type);
    if (!Schema.Type.RECORD.equals(schema.getType())) {
      throw new UnsupportedTypeException("type must be a RECORD but is " + schema.getType().name());
    }
    StringWriter writer = new StringWriter();
    writer.append('(');
    generateRecordSchema(schema, writer);
    writer.append(')');
    return writer.toString();
  }

  private static void generateHiveSchema(Schema schema, StringWriter writer) throws UnsupportedTypeException {

    switch (schema.getType()) {

      case NULL:
        throw new UnsupportedTypeException("Null schema not supported.");
      case BOOLEAN:
        writer.append("BOOLEAN");
        break;
      case INT:
        writer.append("INT");
        break;
      case LONG:
        writer.append("BIGINT");
        break;
      case FLOAT:
        writer.append("FLOAT");
        break;
      case DOUBLE:
        writer.append("DOUBLE");
        break;
      case BYTES:
        writer.append("BINARY");
        break;
      case STRING:
        writer.append("STRING");
        break;
      case ENUM:
        writer.append("STRING");
        break;
      case ARRAY:
        writer.append("ARRAY<");
        generateHiveSchema(schema.getComponentSchema(), writer);
        writer.append('>');
        break;
      case MAP:
        writer.append("MAP<");
        generateHiveSchema(schema.getMapSchema().getKey(), writer);
        writer.append(',');
        generateHiveSchema(schema.getMapSchema().getValue(), writer);
        writer.append('>');
        break;
      case RECORD:
        writer.append("RECORD<");
        generateRecordSchema(schema.getMapSchema().getValue(), writer);
        writer.append('>');
        break;
      case UNION:
        List<Schema> subSchemas = schema.getUnionSchemas();
        if (subSchemas.size() == 2 && Schema.Type.NULL.equals(subSchemas.get(1).getType())) {
          generateHiveSchema(subSchemas.get(0), writer);
          break;
        }
        writer.append("UNIONTYPE<");
        boolean first = true;
        for (Schema subSchema : schema.getUnionSchemas()) {
          if (!first) {
            writer.append(",");
          } else {
            first = false;
          }
          generateHiveSchema(subSchema, writer);
        }
        writer.append(">");
        break;
    }

  }

  private static void generateRecordSchema(Schema schema, StringWriter writer) throws UnsupportedTypeException {
    boolean first = true;
    for (Schema.Field field : schema.getFields()) {
      if (!first) {
        writer.append(',');
      } else {
        first = false;
      }
      writer.append(field.getName());
      writer.append(' ');
      generateHiveSchema(field.getSchema(), writer);
    }
  }

}
