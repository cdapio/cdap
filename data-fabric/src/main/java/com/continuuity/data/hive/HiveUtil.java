package com.continuuity.data.hive;

import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.continuuity.internal.io.Schema;
import com.continuuity.internal.io.UnsupportedTypeException;

import java.io.StringWriter;
import java.lang.reflect.Type;
import java.util.List;

/**
 *
 */
public class HiveUtil {

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
          writer.append("VARCHAR");
          break;
        case ENUM:
          writer.append("VARCHAR");
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
      writer.append(':');
      generateHiveSchema(field.getSchema(), writer);
    }
  }


}
