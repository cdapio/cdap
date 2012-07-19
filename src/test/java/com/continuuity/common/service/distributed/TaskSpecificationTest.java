package com.continuuity.common.service.distributed;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 *
 */
public class TaskSpecificationTest {

  /**
   * Tests builder for TaskSpecification
   *
   * @throws Exception
   */
  @Test
  public void testBasicTaskSpecification() throws Exception {
    Configuration conf = new Configuration();
    TaskSpecification ts
      = new TaskSpecification.Builder(conf)
              .setId("123")
              .setUser("A")
              .setNumInstances(2)
              .setMemory(1024)
              .addEnv("A", "B")
              .addEnv("C", "D")
              .addCommand("ls -ltr")
              .create();
    Assert.assertTrue(ts.getMemory() == 1024);
    Assert.assertTrue(ts.getEnvironment().size() == 2);
    Assert.assertTrue("123".equals(ts.getId()));
    Assert.assertTrue("A".equals(ts.getUser()));
    Assert.assertTrue(ts.getNumInstances() == 2);
    Assert.assertTrue(ts.getMemory() == 1024);
  }

}
