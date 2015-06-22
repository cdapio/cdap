package co.cask.cdap.template.etl.transform;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.template.etl.common.StructuredToAvroTransformer;
import co.cask.cdap.template.etl.common.AvroToStructuredTransformer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Assert;
import org.junit.Test;

public class StructuredtoAvroTest {

  @Test
  public void testStructuredToAvroConversionForNested() throws Exception {
    Schema innerSchema = Schema.recordOf(
      "inner",
      Schema.Field.of("innerInt", Schema.of(Schema.Type.INT)),
      Schema.Field.of("innerString", Schema.of(Schema.Type.STRING)));
    Schema schema = Schema.recordOf(
      "event",
      Schema.Field.of("intField", Schema.of(Schema.Type.INT)),
      Schema.Field.of("recordField", innerSchema));

    StructuredRecord record = StructuredRecord.builder(schema)
      .set("intField", 5)
      .set("recordField",
           StructuredRecord.builder(innerSchema)
             .set("innerInt", 7)
             .set("innerString", "hello world")
             .build()
      )
      .build();
    StructuredToAvroTransformer structuredToAvroTransformer = new StructuredToAvroTransformer();
    GenericRecord result = structuredToAvroTransformer.transform(record);
    Assert.assertEquals(5, result.get("intField"));
    GenericRecord innerRecord = (GenericRecord) result.get("recordField");
    Assert.assertEquals(7, innerRecord.get("innerInt"));
    Assert.assertEquals("hello world", innerRecord.get("innerString"));
  }

  @Test
  public void testAvroToStructuredConversionForNested() throws Exception {
    AvroToStructuredTransformer avroToStructuredTransformer = new AvroToStructuredTransformer();
    Schema innerSchema = Schema.recordOf(
      "inner",
      Schema.Field.of("int", Schema.of(Schema.Type.INT)),
      Schema.Field.of("double", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("array", Schema.arrayOf(Schema.of(Schema.Type.FLOAT)))
      // uncomment this line once [CDAP - 2813 is fixed]. You might have to fix AvroToStructuredTransformer.java
      // Schema.Field.of("map", Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING)))
    );

    Schema schema = Schema.recordOf(
      "record",
      Schema.Field.of("int", Schema.of(Schema.Type.INT)),
      Schema.Field.of("record", innerSchema));

    org.apache.avro.Schema avroInnerSchema = convertSchema(innerSchema);
    org.apache.avro.Schema avroSchema = convertSchema(schema);

    GenericRecord record = new GenericRecordBuilder(avroSchema)
      .set("int", Integer.MAX_VALUE)
      .set("record",
           new GenericRecordBuilder(avroInnerSchema)
             .set("int", 5)
             .set("double", 3.14159)
             .set("array", ImmutableList.of(1.0f, 2.0f))
             // uncomment this line once [CDAP - 2813 is fixed]. You might have to fix AvroToStructuredTransformer.java
             // .set("map", ImmutableMap.of("key", "value"))
             .build())
      .build();

    StructuredRecord result = avroToStructuredTransformer.transform(record);
    Assert.assertEquals(Integer.MAX_VALUE, result.get("int"));
    StructuredRecord innerResult = result.get("record");
    Assert.assertEquals(5, innerResult.get("int"));
    Assert.assertEquals(ImmutableList.of(1.0f, 2.0f), (ImmutableList)innerResult.get("array"));
  }
  private org.apache.avro.Schema convertSchema(Schema cdapSchema) {
    return new org.apache.avro.Schema.Parser().parse(cdapSchema.toString());
  }
}
