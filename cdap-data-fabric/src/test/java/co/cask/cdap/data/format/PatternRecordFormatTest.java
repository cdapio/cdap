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
package co.cask.cdap.data.format;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 *
 */
public class PatternRecordFormatTest {

  @Test
  public void testFoo() throws Exception {
    PatternRecordFormat format = new PatternRecordFormat();
    Schema schema = Schema.recordOf("streamEvent",
                    Schema.Field.of("user", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
                    ,Schema.Field.of("body", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
    );

    FormatSpecification spec = new FormatSpecification(PatternRecordFormat.class.getCanonicalName(),
                                                       schema,
                                                       ImmutableMap.<String, String>of
                                                         ("pattern", "%{USER:user}:%{GREEDYDATA:body}"));
    format.initialize(spec);
    String data = "nitin:falkfjaksjf fkafjalkf fa fasfsalfsaf af afaslkfjasf asf af asf";
    StructuredRecord output = format.read(new StreamEvent(ByteBuffer.wrap(Bytes.toBytes(data))));

    Assert.assertEquals("nitin", output.get("user"));
    //Assert.assertEquals("Hello", output.get("body"));
  }

}
