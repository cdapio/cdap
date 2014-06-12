package com.continuuity.hive;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test dynamic configuration setting.
 */
public class HiveConfTest {
  @Test
  public void testDynamicConf() throws Exception {
    System.clearProperty(HiveConf.ConfVars.HIVEAUXJARS.toString());
    System.clearProperty(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT.toString());

    HiveConf hiveConf = new HiveConf();
    Assert.assertNull(hiveConf.getAuxJars());
    Assert.assertEquals(10000, hiveConf.getInt(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT.toString(), 10000));

    System.setProperty(HiveConf.ConfVars.HIVEAUXJARS.toString(), "file:/some/file/path");
    System.setProperty(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT.toString(), "45678");

    hiveConf = new HiveConf();
    Assert.assertEquals("file:/some/file/path", hiveConf.getAuxJars());
    Assert.assertEquals(45678, hiveConf.getInt("hive.server2.thrift.port", 0));

    System.clearProperty(HiveConf.ConfVars.HIVEAUXJARS.toString());
    System.clearProperty(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT.toString());
  }
}
