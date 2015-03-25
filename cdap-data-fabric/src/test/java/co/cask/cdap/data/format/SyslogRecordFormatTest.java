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
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 *
 */
public class SyslogRecordFormatTest {

  @Test
  public void testSyslog() throws UnsupportedTypeException {
    SyslogRecordFormat format = createSyslogFormat();

    String message = "Oct 17 08:59:00 suod newsyslog[6215]: logfile turned over";
    StructuredRecord record = applyFormat(format, message);
    Assert.assertEquals("Oct 17 08:59:00", record.get("timestamp"));
    Assert.assertEquals("suod", record.get("logsource"));
    Assert.assertEquals("newsyslog", record.get("program"));
    Assert.assertEquals("6215", record.get("pid"));
    Assert.assertEquals("logfile turned over", record.get("message"));

    message = "Oct 17 08:59:04 cdr.cs.colorado.edu amd[29648]: " +
      "noconn option exists, and was turned on! (May cause NFS hangs on some systems...)";
    record = applyFormat(format, message);
    Assert.assertEquals("Oct 17 08:59:04", record.get("timestamp"));
    Assert.assertEquals("cdr.cs.colorado.edu", record.get("logsource"));
    Assert.assertEquals("amd", record.get("program"));
    Assert.assertEquals("29648", record.get("pid"));
    Assert.assertEquals("noconn option exists, and was turned on! (May cause NFS hangs on some systems...)",
                        record.get("message"));
  }

  private StructuredRecord applyFormat(SyslogRecordFormat format, String input) {
    return format.read(new StreamEvent(ByteBuffer.wrap(Bytes.toBytes(input))));
  }

  private SyslogRecordFormat createSyslogFormat() throws UnsupportedTypeException {
    SyslogRecordFormat format = new SyslogRecordFormat();
    FormatSpecification spec = new FormatSpecification(SyslogRecordFormat.class.getCanonicalName(), null, null);
    format.initialize(spec);
    return format;
  }

}
