package com.continuuity.hive;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.hive.context.CConfCodec;
import com.continuuity.hive.context.ConfigurationUtil;
import com.continuuity.hive.context.HConfCodec;

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
