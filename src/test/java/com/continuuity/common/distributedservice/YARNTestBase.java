package com.continuuity.common.distributedservice;

import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 *
 *
 */
public class YARNTestBase {

  private static MiniYARNCluster yarnCluster;

  @BeforeClass
  public static void setup() throws Exception {
    yarnCluster = new MiniYARNCluster("inmemyarn", 5, 2, 2);
  }

  @AfterClass
  public static void teardown() {
    if(yarnCluster != null) {
      yarnCluster.stop();
      yarnCluster = null;
    }
  }
}
