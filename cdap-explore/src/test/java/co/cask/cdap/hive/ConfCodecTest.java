/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.hive;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.hive.context.CConfCodec;
import co.cask.cdap.hive.context.ConfigurationUtil;
import co.cask.cdap.hive.context.HConfCodec;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 *
 */
public class ConfCodecTest {

  @Test
  public void testCConfCodec() throws Exception {
    // Serialize
    CConfiguration conf = CConfiguration.create();
    conf.set("foo", "bar");

    Configuration hconf = HBaseConfiguration.create();
    hconf.set("hfoo", "hbar");

    Map<String, String> confMap = Maps.newHashMap();
    ConfigurationUtil.set(confMap, Constants.Explore.CCONF_KEY, CConfCodec.INSTANCE, conf);
    ConfigurationUtil.set(confMap, Constants.Explore.HCONF_KEY, HConfCodec.INSTANCE, hconf);

    // Deserialize
    CConfiguration newConf = ConfigurationUtil.get(confMap, Constants.Explore.CCONF_KEY, CConfCodec.INSTANCE);
    Assert.assertEquals("bar", newConf.get("foo"));

    Configuration newHconf = ConfigurationUtil.get(confMap, Constants.Explore.HCONF_KEY, HConfCodec.INSTANCE);
    Assert.assertEquals("hbar", newHconf.get("hfoo"));
  }
}
