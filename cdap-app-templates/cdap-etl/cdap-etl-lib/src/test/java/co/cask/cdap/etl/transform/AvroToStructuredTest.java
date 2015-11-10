/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.etl.transform;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.common.AvroToStructuredTransformer;
import com.google.common.collect.ImmutableList;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class AvroToStructuredTest {
  @Test
  public void testAvroToStructuredConversionForNested() throws Exception {
    AvroToStructuredTransformer avroToStructuredTransformer = new AvroToStructuredTransformer();

    Schema innerNestedSchema = Schema.recordOf("innerNested",
                                               Schema.Field.of("int", Schema.of(Schema.Type.INT)));
    Schema innerSchema = Schema.recordOf(
      "inner",
      Schema.Field.of("int", Schema.of(Schema.Type.INT)),
      Schema.Field.of("double", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("array", Schema.arrayOf(Schema.of(Schema.Type.FLOAT))),
      Schema.Field.of("array1", Schema.arrayOf(innerNestedSchema))

      // uncomment this line once [CDAP - 2813 is fixed]. You might have to fix AvroToStructuredTransformer.java
      // Schema.Field.of("map", Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING)))
    );

    Schema schema = Schema.recordOf(
      "record",
      Schema.Field.of("int", Schema.of(Schema.Type.INT)),
      Schema.Field.of("record", innerSchema));


    org.apache.avro.Schema avroInnerSchema = convertSchema(innerSchema);
    org.apache.avro.Schema avroSchema = convertSchema(schema);
    org.apache.avro.Schema avroInnerNestedSchema = convertSchema(innerNestedSchema);


    GenericRecord inner = new GenericRecordBuilder(avroInnerNestedSchema)
      .set("int", 0)
      .build();
    GenericRecord record = new GenericRecordBuilder(avroSchema)
      .set("int", Integer.MAX_VALUE)
      .set("record",
           new GenericRecordBuilder(avroInnerSchema)
             .set("int", 5)
             .set("double", 3.14159)
             .set("array", ImmutableList.of(1.0f, 2.0f))
             .set("array1", ImmutableList.of(inner, inner))
               // uncomment this line once [CDAP - 2813 is fixed]. You might have to fix AvroToStructuredTransformer
               // .set("map", ImmutableMap.of("key", "value"))
             .build())
      .build();

    StructuredRecord result = avroToStructuredTransformer.transform(record);
    Assert.assertEquals(Integer.MAX_VALUE, result.get("int"));
    StructuredRecord innerResult = result.get("record");
    Assert.assertEquals(5, innerResult.get("int"));
    Assert.assertEquals(ImmutableList.of(1.0f, 2.0f), innerResult.get("array"));
    List list = innerResult.get("array1");
    StructuredRecord array1Result = (StructuredRecord) list.get(0);
    Assert.assertEquals(0, array1Result.get("int"));
  }

  private org.apache.avro.Schema convertSchema(Schema cdapSchema) {
    return new org.apache.avro.Schema.Parser().parse(cdapSchema.toString());
  }
}
