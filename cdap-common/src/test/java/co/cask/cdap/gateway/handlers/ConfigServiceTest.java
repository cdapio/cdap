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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.proto.ConfigEntry;
import org.apache.commons.io.input.ReaderInputStream;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import java.io.StringReader;
import java.util.List;

/**
 *
 */
public class ConfigServiceTest {

  @Test
  public void testConfig() {

    // cConf
    CConfiguration cConf = CConfiguration.create();
    cConf.clear();

    String cConfResourceString =
      "<configuration>\n" +
        "\n" +
        "  <property>\n" +
        "    <name>stream.zz.threshold</name>\n" +
        "    <value>1</value>\n" +
        "    <description>Some description</description>\n" +
        "  </property>\n" +
        "\n" +
        "</configuration>";
    ReaderInputStream cConfResource = new ReaderInputStream(new StringReader(cConfResourceString));
    cConf.addResource(cConfResource);

    ConfigEntry cConfEntry = new ConfigEntry(
      "stream.zz.threshold", "1", cConfResource.toString());

    // hConf
    Configuration hConf = new Configuration();
    String hConfResourceString =
      "<configuration>\n" +
      "\n" +
      "  <property>\n" +
      "    <name>stream.notification.threshold</name>\n" +
      "    <value>3</value>\n" +
      "    <description>Some description</description>\n" +
      "  </property>\n" +
      "\n" +
      "</configuration>";
    ReaderInputStream hConfResource = new ReaderInputStream(new StringReader(hConfResourceString));
    hConf.addResource(hConfResource);

    ConfigEntry hConfEntry = new ConfigEntry(
      "stream.notification.threshold", "3", hConfResource.toString());

    // test
    ConfigService configService = new ConfigService(cConf, hConf);
    List<ConfigEntry> cConfEntries = configService.getCConf();
    Assert.assertTrue(cConfEntries.contains(cConfEntry));

    List<ConfigEntry> hConfEntries = configService.getHConf();
    Assert.assertTrue(hConfEntries.contains(hConfEntry));
  }
}
