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
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * SysLogRecordFormat Test.
 */
public class SyslogRecordFormatTest {

  @Test
  public void testRFC5424SysLogFormat() throws Exception {
    SyslogRecordFormat format = new SyslogRecordFormat();
    FormatSpecification spec = new FormatSpecification(SyslogRecordFormat.class.getCanonicalName(),
                                                       null, ImmutableMap.<String, String>of());
    format.initialize(spec);
    String data = "<187>Nov 19 02:58:57 nms-server6 %cgmesh-2-outage: Outage detected on this device";
    data = "Mar 22 07:36:09 Joltie-Air.local sharingd[356] <Notice>: 07:36:09.879 : BTLE scanning started";
    StructuredRecord output = format.read(new StreamEvent(ByteBuffer.wrap(Bytes.toBytes(data))));

    Assert.assertEquals(187, output.get("priority"));
    Assert.assertEquals("nms-server6", output.get("hostname"));
  }
}
