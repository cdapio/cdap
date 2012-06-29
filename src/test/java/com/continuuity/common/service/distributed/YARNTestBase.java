package com.continuuity.common.service.distributed;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;

/**
 *
 *
 */
public class YARNTestBase {
  private static final Logger Log = LoggerFactory.getLogger(YARNTestBase.class);
  protected static MiniYARNCluster yarnCluster = null;
  protected static Configuration conf = new Configuration();

  @BeforeClass
  public static void setup() throws InterruptedException, IOException {
    //conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 128);
    if (yarnCluster == null) {
      yarnCluster = new MiniYARNCluster(YARNTestBase.class.getName(), 2, 1, 1);
      yarnCluster.init(conf);
      yarnCluster.start();
    }
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      Log.info("setup thread sleep interrupted. message=" + e.getMessage());
    }
  }

  @AfterClass
  public static void teardown() throws IOException {
    if(yarnCluster != null) {
      yarnCluster.stop();
      yarnCluster = null;
    }
  }

  public Configuration getConfiguration() {
    return conf;
  }
}
