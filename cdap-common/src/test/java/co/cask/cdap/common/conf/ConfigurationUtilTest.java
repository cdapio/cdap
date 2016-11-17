/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.common.conf;

import co.cask.cdap.common.io.Codec;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 * Tests for {@link ConfigurationUtil}.
 */
public class ConfigurationUtilTest {

  @Test
  public void testMultipleMacros() throws IOException {
    Configuration conf = new Configuration();
    Configuration confToEncode = new Configuration();

    confToEncode.set("my.key", "my.value");
    for (int i = 0; i < 30; i++) {
      confToEncode.set("key.num." + i, "${my.key}");
    }
    ConfigurationUtil.set(conf, "test.key", HConfCodec.INSTANCE, confToEncode);
//    conf.set("my.key", "foo");
    Configuration decodedConf = ConfigurationUtil.get(conf, "test.key", HConfCodec.INSTANCE);
    int i = 2;
  }

  /**
   * Codec to encode/decode HBase Configuration object.
   */
  public static class HConfCodec implements Codec<org.apache.hadoop.conf.Configuration> {
    public static final HConfCodec INSTANCE = new HConfCodec();

    private HConfCodec() {
      // Use the static INSTANCE to get an instance.
    }

    @Override
    public byte[] encode(org.apache.hadoop.conf.Configuration object) throws IOException {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      object.writeXml(bos);
      bos.close();
      return bos.toByteArray();
    }

    @Override
    public org.apache.hadoop.conf.Configuration decode(byte[] data) throws IOException {
      if (data == null) {
        return new Configuration();
//        return HBaseConfiguration.create();
      }

      ByteArrayInputStream bin = new ByteArrayInputStream(data);
      org.apache.hadoop.conf.Configuration hConfiguration = new org.apache.hadoop.conf.Configuration();
      hConfiguration.addResource(bin);
      return hConfiguration;
    }
  }


  @Test
  public void testNamedConfigurations() throws IOException {
    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();

    Map<String, String> name1Config = ImmutableMap.of("key1", "value1",
                                                      "key2", "value2");
    Map<String, String> name2Config = ImmutableMap.of("name2key", "name2value");
    Map<String, String> nameDotConfig = ImmutableMap.of("name3key", "name3value");
    Map<String, String> emptyConfig = ImmutableMap.of();

    ConfigurationUtil.setNamedConfigurations(conf, "name1", name1Config);
    ConfigurationUtil.setNamedConfigurations(conf, "name2", name2Config);
    ConfigurationUtil.setNamedConfigurations(conf, "name.", nameDotConfig);
    ConfigurationUtil.setNamedConfigurations(conf, "emptyConfig", emptyConfig);


    Assert.assertEquals(name1Config, ConfigurationUtil.getNamedConfigurations(conf, "name1"));
    Assert.assertEquals(name2Config, ConfigurationUtil.getNamedConfigurations(conf, "name2"));
    Assert.assertEquals(nameDotConfig, ConfigurationUtil.getNamedConfigurations(conf, "name."));
    Assert.assertEquals(emptyConfig, ConfigurationUtil.getNamedConfigurations(conf, "emptyConfig"));
  }

}
