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

package co.cask.cdap.templates.etl.transforms;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.templates.etl.api.Emitter;
import co.cask.cdap.templates.etl.api.StageSpecification;
import co.cask.cdap.templates.etl.api.TransformContext;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * Tests {@link StructuredRecordToAvroTransform#transform(LongWritable, StructuredRecord, Emitter)}
 */
public class StructuredRecordToAvroTransformTest {

  Schema eventSchema = Schema.recordOf(
    "event",
    Schema.Field.of("field1", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("field2", Schema.of(Schema.Type.INT)),
    Schema.Field.of("field3", Schema.of(Schema.Type.DOUBLE)));

  @Test
  public void testStructuredRecordToAvroTransform() throws Exception {

    StructuredRecordToAvroTransform transformer = new StructuredRecordToAvroTransform();

    StructuredRecord.Builder builder = StructuredRecord.builder(eventSchema);
    builder.set("field1", "string1");
    builder.set("field2", 2);
    builder.set("field3", 3.0);
    StructuredRecord structuredRecord = builder.build();

    TransformContext transformContext = new TransformContext() {
      @Override
      public StageSpecification getSpecification() {
        return null;
      }

      @Override
      public Map<String, String> getRuntimeArguments() {
        return null;
      }
    };
    transformer.initialize(transformContext);
    Emitter<AvroKey<GenericRecord>, NullWritable> emitter = new Emitter<AvroKey<GenericRecord>, NullWritable>() {
      @Override
      public void emit(AvroKey<GenericRecord> key, NullWritable value) {
        Assert.assertEquals("string1", key.datum().get("field1"));
        Assert.assertEquals(2, key.datum().get("field2"));
        Assert.assertEquals(3.0, key.datum().get("field3"));
      }
    };
    transformer.transform(null, structuredRecord, emitter);
  }
}
