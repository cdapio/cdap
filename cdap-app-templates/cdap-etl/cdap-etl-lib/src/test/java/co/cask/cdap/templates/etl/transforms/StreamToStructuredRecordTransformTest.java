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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.Formats;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.templates.etl.api.Emitter;
import co.cask.cdap.templates.etl.api.StageContext;
import co.cask.cdap.templates.etl.common.MockEmitter;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * Tests {@link StreamToStructuredRecordTransform#transform(StreamEvent, Emitter)}
 */
public class StreamToStructuredRecordTransformTest {

  Schema eventSchema = Schema.recordOf(
    "event",
    Schema.Field.of("field1", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("field2", Schema.of(Schema.Type.INT)),
    Schema.Field.of("field3", Schema.of(Schema.Type.DOUBLE)));

  @Test
  public void testStreamToStrucuturedRecordTransform() throws Exception {

    StreamToStructuredRecordTransform transformer = new StreamToStructuredRecordTransform();
    StageContext transformContext = new MockTransformContext(
      ImmutableMap.of("format.name", Formats.CSV, "schema", eventSchema.toString()));
    transformer.initialize(transformContext);
    StreamEvent streamEvent = new StreamEvent(ImmutableMap.of("header1", "one"),
                                              ByteBuffer.wrap(Bytes.toBytes("String1,2,3.0")));
    MockEmitter<StructuredRecord> emitter = new MockEmitter<StructuredRecord>();
    transformer.transform(streamEvent, emitter);
    StructuredRecord value = emitter.getEmitted().get(0);
    Assert.assertNotNull(value.get("headers"));
    Assert.assertEquals("String1", value.get("field1"));
    Assert.assertEquals(2, value.get("field2"));
    Assert.assertEquals(3.0, value.get("field3"));
  }
}
