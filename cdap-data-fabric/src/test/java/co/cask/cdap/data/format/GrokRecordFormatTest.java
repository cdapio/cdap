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
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 *
 */
public class GrokRecordFormatTest {

  @Test
  public void testSimple() throws UnsupportedTypeException {
    Schema schema = Schema.recordOf(
      "streamEvent",
      Schema.Field.of("user", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("body", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    GrokRecordFormat format = createGrokFormat(schema, "%{USER:user}:%{GREEDYDATA:body}");

    StructuredRecord record = applyFormat(
      format, "nitin:falkfjaksjf fkafjalkf fa fasfsalfsaf af afaslkfjasf asf af asf");
    Assert.assertEquals("nitin", record.get("user"));
    Assert.assertEquals("falkfjaksjf fkafjalkf fa fasfsalfsaf af afaslkfjasf asf af asf", record.get("body"));
  }

  @Test
  public void testDefault() throws UnsupportedTypeException {
    GrokRecordFormat format = createGrokFormat(null, "%{GREEDYDATA:body}");

    String message = "Oct 17 08:59:00 suod newsyslog[6215]: logfile turned over";
    StructuredRecord record = applyFormat(format, message);
    Assert.assertEquals("Oct 17 08:59:00 suod newsyslog[6215]: logfile turned over", record.get("body"));
  }

  private StructuredRecord applyFormat(GrokRecordFormat format, String input) {
    return format.read(new StreamEvent(ByteBuffer.wrap(Bytes.toBytes(input))));
  }

  private GrokRecordFormat createGrokFormat(Schema schema, String pattern) throws UnsupportedTypeException {
    GrokRecordFormat format = new GrokRecordFormat();
    FormatSpecification spec = new FormatSpecification(GrokRecordFormat.class.getCanonicalName(),
                                                       schema, GrokRecordFormat.settings(pattern));
    format.initialize(spec);
    return format;
  }

}
