package com.continuuity.common.service.distributed;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 *
 */
public class ContainerGroupSpecificationTest {

  @Test
  public void testBasicGroupParameter() throws Exception {
    Configuration conf = new Configuration();
    ContainerGroupSpecification cgp
      = new ContainerGroupSpecification.Builder(conf)
              .setMemory(1024)
              .addEnv("A", "B")
              .addEnv("C", "D")
              .addCommand("ls -ltr")
              .create();
    Assert.assertTrue(cgp.getMemory() == 1024);
    Assert.assertTrue(cgp.getEnvironment().size() == 2);
  }

}
