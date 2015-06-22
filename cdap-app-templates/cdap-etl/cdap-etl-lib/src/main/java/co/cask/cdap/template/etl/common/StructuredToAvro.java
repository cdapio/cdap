package co.cask.cdap.template.etl.common;

import co.cask.cdap.api.data.format.StructuredRecord;
import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.io.IOException;
import java.util.Map;

/**
 * Creates GenericRecords from StructuredRecords
 */
public class StructuredToAvro extends Converter<StructuredRecord, GenericRecord> {

  private final Map<Integer, Schema> schemaCache = Maps.newHashMap();

  public GenericRecord convert(StructuredRecord structuredRecord) throws IOException {
    co.cask.cdap.api.data.schema.Schema structuredRecordSchema = structuredRecord.getSchema();

    int hashCode = structuredRecordSchema.hashCode();
    Schema avroSchema;

    if (schemaCache.containsKey(hashCode)) {
      avroSchema = schemaCache.get(hashCode);
    } else {
      avroSchema = new Schema.Parser().parse(structuredRecordSchema.toString());
      schemaCache.put(hashCode, avroSchema);
    }

    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(avroSchema);
    for (Schema.Field field : avroSchema.getFields()) {
      String fieldName = field.name();
      recordBuilder.set(fieldName, convertField(structuredRecord.get(fieldName), field.schema()));
    }
    return recordBuilder.build();
  }

  protected GenericRecord convertRecord (StructuredRecord structuredRecord) throws IOException {
    co.cask.cdap.api.data.schema.Schema structuredRecordSchema = structuredRecord.getSchema();
    int hashCode = structuredRecordSchema.hashCode();
    Schema avroSchema;

    if (schemaCache.containsKey(hashCode)) {
      avroSchema = schemaCache.get(hashCode);
    } else {
      avroSchema = new Schema.Parser().parse(structuredRecordSchema.toString());
      schemaCache.put(hashCode, avroSchema);
    }

    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(avroSchema);
    for (Schema.Field field : avroSchema.getFields()) {
      String fieldName = field.name();
      Schema fieldSchema = field.schema();
      Object fieldValue = getRecordField(structuredRecord, fieldName);
      recordBuilder.set(fieldName, convertField(fieldValue, fieldSchema));
    }
    return recordBuilder.build();
  }
}
