package co.cask.cdap.template.etl.common;

import co.cask.cdap.api.data.format.StructuredRecord;
import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class AvroToStructured extends Converter<GenericRecord, StructuredRecord> {

  private final Map<Integer, co.cask.cdap.api.data.schema.Schema> schemaCache = Maps.newHashMap();

  @Override
  public StructuredRecord convert(GenericRecord genericRecord) throws IOException {
    Schema genericRecordSchema = genericRecord.getSchema();

    int hashCode = genericRecordSchema.hashCode();
    co.cask.cdap.api.data.schema.Schema structuredSchema;

    if (schemaCache.containsKey(hashCode)) {
      structuredSchema = schemaCache.get(hashCode);
    } else {
      structuredSchema = co.cask.cdap.api.data.schema.Schema.parseJson(genericRecordSchema.toString());
      schemaCache.put(hashCode, structuredSchema);
    }

    StructuredRecord.Builder builder = StructuredRecord.builder(structuredSchema);
    for (Schema.Field field : genericRecordSchema.getFields()) {
      String fieldName = field.name();
      builder.set(fieldName, convertField(genericRecord.get(fieldName), field.schema()));
    }

    return builder.build();
  }

  @Override
  protected StructuredRecord convertRecord(GenericRecord genericRecord) throws IOException {
    Schema avroSchema = genericRecord.getSchema();

    int hashCode = avroSchema.hashCode();
    co.cask.cdap.api.data.schema.Schema structuredSchema;

    if (schemaCache.containsKey(hashCode)) {
      structuredSchema = schemaCache.get(hashCode);
    } else {
      structuredSchema = co.cask.cdap.api.data.schema.Schema.parseJson(avroSchema.toString());
      schemaCache.put(hashCode, structuredSchema);
    }

    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(structuredSchema);
    for (Schema.Field field : avroSchema.getFields()) {
      String fieldName = field.name();
      Schema fieldSchema = field.schema();
      Object fieldValue = getRecordField(genericRecord, fieldName);
      recordBuilder.set(fieldName, convertField(fieldValue, fieldSchema));
    }
    return recordBuilder.build();
  }


}
