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
import co.cask.cdap.api.data.format.UnexpectedFormatException;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * CombinedLogFormat tests.
 */
public class CombinedLogRecordFormatTest {
  @Test
  public void testCLFLogWithNull() throws UnsupportedTypeException, UnexpectedFormatException {
    CombinedLogRecordFormat format = new CombinedLogRecordFormat();
    FormatSpecification spec = new FormatSpecification(CombinedLogRecordFormat.class.getCanonicalName(),
                                                       null, ImmutableMap.<String, String>of());
    format.initialize(spec);
    String data = "10.10.10.10 - - [01/Feb/2015:09:58:24 +0000] \"-\" 408 - \"-\" \"-\"";
    StructuredRecord output = format.read(new StreamEvent(ByteBuffer.wrap(Bytes.toBytes(data))));

    Assert.assertEquals("10.10.10.10", output.get("remote_host"));
    Assert.assertNull(output.get("remote_login"));
    Assert.assertNull(output.get("auth_user"));
    Assert.assertEquals("01/Feb/2015:09:58:24 +0000", output.get("date"));
    Assert.assertNull(output.get("request"));
    Assert.assertEquals(408, output.get("status"));
    Assert.assertNull(output.get("content_length"));
    Assert.assertNull(output.get("referrer"));
    Assert.assertNull(output.get("user_agent"));
  }

  @Test
  public void testCLFLog() throws UnsupportedTypeException, UnexpectedFormatException {
    CombinedLogRecordFormat format = new CombinedLogRecordFormat();
    FormatSpecification spec = new FormatSpecification(CombinedLogRecordFormat.class.getCanonicalName(),
                                                       null, ImmutableMap.<String, String>of());
    format.initialize(spec);
    String data = "10.10.10.10 - - [01/Feb/2015:06:47:10 +0000] \"GET /browse/COOP-DBT-JOB1-238/artifact HTTP/1.1\"" +
      " 301 256 \"-\" \"Mozilla/5.0 (compatible; AhrefsBot/5.0; +http://ahrefs.com/robot/)\"";
    StructuredRecord output = format.read(new StreamEvent(ByteBuffer.wrap(Bytes.toBytes(data))));

    Assert.assertEquals("10.10.10.10", output.get("remote_host"));
    Assert.assertNull(output.get("remote_login"));
    Assert.assertNull(output.get("auth_user"));
    Assert.assertEquals("01/Feb/2015:06:47:10 +0000", output.get("date"));
    Assert.assertEquals("GET /browse/COOP-DBT-JOB1-238/artifact HTTP/1.1", output.get("request"));
    Assert.assertEquals(301, output.get("status"));
    Assert.assertEquals(256, output.get("content_length"));
    Assert.assertNull(output.get("referrer"));
    Assert.assertEquals("Mozilla/5.0 (compatible; AhrefsBot/5.0; +http://ahrefs.com/robot/)", output.get("user_agent"));
  }

  @Test
  public void testCLFLogWithEscapedDoubleQuotes() throws UnsupportedTypeException, UnexpectedFormatException {
    CombinedLogRecordFormat format = new CombinedLogRecordFormat();
    FormatSpecification spec = new FormatSpecification(CombinedLogRecordFormat.class.getCanonicalName(),
                                                       null, ImmutableMap.<String, String>of());
    format.initialize(spec);
    String data = "10.10.10.10 - - [01/Feb/2015:06:38:58 +0000] \"GET /plugins/servlet/buildStatusImage/CDAP-DUT " +
      "HTTP/1.1\" 301 257 \"http://cdap.io/\" \"\\\"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, " +
      "like Gecko) Chrome/31.0.1650.57 Safari/537.36 OPR/18.0.1284.49\\\"\"";
    StructuredRecord output = format.read(new StreamEvent(ByteBuffer.wrap(Bytes.toBytes(data))));

    Assert.assertEquals("10.10.10.10", output.get("remote_host"));
    Assert.assertNull(output.get("remote_login"));
    Assert.assertNull(output.get("auth_user"));
    Assert.assertEquals("01/Feb/2015:06:38:58 +0000", output.get("date"));
    Assert.assertEquals("GET /plugins/servlet/buildStatusImage/CDAP-DUT HTTP/1.1", output.get("request"));
    Assert.assertEquals(301, output.get("status"));
    Assert.assertEquals(257, output.get("content_length"));
    Assert.assertEquals("http://cdap.io/", output.get("referrer"));
    Assert.assertEquals("\\\"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) " +
                          "Chrome/31.0.1650.57 Safari/537.36 OPR/18.0.1284.49\\\"", output.get("user_agent"));
  }


  @Test(expected = UnexpectedFormatException.class)
  public void testInvalid() throws UnsupportedTypeException, UnexpectedFormatException {
    CombinedLogRecordFormat format = new CombinedLogRecordFormat();
    FormatSpecification spec = new FormatSpecification(CombinedLogRecordFormat.class.getCanonicalName(),
                                                       null, ImmutableMap.<String, String>of());
    format.initialize(spec);
    String data = "10.10.10.10[01/Feb/2015:06:47:10 +0000";
    StructuredRecord output = format.read(new StreamEvent(ByteBuffer.wrap(Bytes.toBytes(data))));
  }
}
