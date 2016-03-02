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
package co.cask.cdap.format;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.format.Formats;
import co.cask.cdap.api.data.format.RecordFormat;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;

/**
 *
 */
public class GrokRecordFormatTest {

  @Test
  public void testSimple() throws Exception {
    Schema schema = Schema.recordOf(
      "streamEvent",
      Schema.Field.of("user", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("body", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    FormatSpecification spec = new FormatSpecification(Formats.GROK, schema,
                                                       GrokRecordFormat.settings("%{USER:user}:%{GREEDYDATA:body}"));
    RecordFormat<StreamEvent, StructuredRecord> format = RecordFormats.createInitializedFormat(spec);

    String message = "nitin:falkfjaksjf fkafjalkf fa fasfsalfsaf af afaslkfjasf asf af asf";
    StructuredRecord record = format.read(new StreamEvent(ByteBuffer.wrap(Bytes.toBytes(message))));
    Assert.assertEquals("nitin", record.get("user"));
    Assert.assertEquals("falkfjaksjf fkafjalkf fa fasfsalfsaf af afaslkfjasf asf af asf", record.get("body"));
  }

  @Test
  public void testDefault() throws Exception {
    FormatSpecification spec = new FormatSpecification(Formats.GROK, null,
                                                       GrokRecordFormat.settings("%{GREEDYDATA:body}"));
    RecordFormat<StreamEvent, StructuredRecord> format = RecordFormats.createInitializedFormat(spec);

    String message = "Oct 17 08:59:00 suod newsyslog[6215]: logfile turned over";
    StructuredRecord record = format.read(new StreamEvent(ByteBuffer.wrap(Bytes.toBytes(message))));
    Assert.assertEquals("Oct 17 08:59:00 suod newsyslog[6215]: logfile turned over", record.get("body"));
  }

  @Test
  public void testCustomPattern() throws Exception {
    FormatSpecification spec = new FormatSpecification(Formats.GROK, null,
                                                       GrokRecordFormat.settings("(?<body>.*)"));
    RecordFormat<StreamEvent, StructuredRecord> format = RecordFormats.createInitializedFormat(spec);

    String message = "Oct 17 08:59:00 suod newsyslog[6215]: logfile turned over";
    StructuredRecord record = format.read(new StreamEvent(ByteBuffer.wrap(Bytes.toBytes(message))));
    Assert.assertEquals("Oct 17 08:59:00 suod newsyslog[6215]: logfile turned over", record.get("body"));
  }

  @Test
  public void testSyslog() throws Exception {
    FormatSpecification spec = new FormatSpecification(Formats.SYSLOG, null, Collections.<String, String>emptyMap());
    RecordFormat<StreamEvent, StructuredRecord> format = RecordFormats.createInitializedFormat(spec);

    String message = "Oct 17 08:59:00 suod newsyslog[6215]: logfile turned over";
    StructuredRecord record = format.read(new StreamEvent(ByteBuffer.wrap(Bytes.toBytes(message))));
    Assert.assertEquals("Oct 17 08:59:00", record.get("timestamp"));
    Assert.assertEquals("suod", record.get("logsource"));
    Assert.assertEquals("newsyslog", record.get("program"));
    Assert.assertEquals("6215", record.get("pid"));
    Assert.assertEquals("logfile turned over", record.get("message"));

    message = "Oct 17 08:59:04 cdr.cs.colorado.edu amd[29648]: " +
      "noconn option exists, and was turned on! (May cause NFS hangs on some systems...)";
    record = format.read(new StreamEvent(ByteBuffer.wrap(Bytes.toBytes(message))));
    Assert.assertEquals("Oct 17 08:59:04", record.get("timestamp"));
    Assert.assertEquals("cdr.cs.colorado.edu", record.get("logsource"));
    Assert.assertEquals("amd", record.get("program"));
    Assert.assertEquals("29648", record.get("pid"));
    Assert.assertEquals("noconn option exists, and was turned on! (May cause NFS hangs on some systems...)",
                        record.get("message"));
  }

}
