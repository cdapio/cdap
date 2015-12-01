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
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.TransformContext;
import co.cask.cdap.etl.common.MockEmitter;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests {@link StructuredRecordToGenericRecordTransform#transform(StructuredRecord, Emitter)}
 */
public class StructuredRecordToGenericRecordTransformTest {

  Schema eventSchema = Schema.recordOf(
    "event",
    Schema.Field.of("field1", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("field2", Schema.of(Schema.Type.INT)),
    Schema.Field.of("field3", Schema.of(Schema.Type.DOUBLE)));

  @Test
  public void testStructuredRecordToAvroTransform() throws Exception {

    StructuredRecordToGenericRecordTransform transformer = new StructuredRecordToGenericRecordTransform();

    StructuredRecord.Builder builder = StructuredRecord.builder(eventSchema);
    builder.set("field1", "string1");
    builder.set("field2", 2);
    builder.set("field3", 3.0);
    StructuredRecord structuredRecord = builder.build();

    TransformContext transformContext = new MockTransformContext();
    transformer.initialize(transformContext);
    MockEmitter<GenericRecord> emitter = new MockEmitter<>();
    transformer.transform(structuredRecord, emitter);
    GenericRecord value = emitter.getEmitted().get(0);
    Assert.assertEquals("string1", value.get("field1"));
    Assert.assertEquals(2, value.get("field2"));
    Assert.assertEquals(3.0, value.get("field3"));
  }
}
