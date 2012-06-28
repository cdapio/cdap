package com.continuuity.common.distributedservice;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 *
 */
public class ContainerGroupParameterTest {

  @Test
  public void testBasicGroupParameter() throws Exception {
    ContainerGroupParameter cgp
      = new ContainerGroupParameter.Builder()
              .setMemory(1024)
              .addEnv("A", "B")
              .addEnv("C", "D")
              .addCommand("ls -ltr")
              .create();
    Assert.assertTrue(cgp.getMemory() == 1024);
    Assert.assertTrue(cgp.getEnvironment().size() == 2);
  }

}
