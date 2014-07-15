/*
 * Copyright 2012-2014 Continuuity, Inc.
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
