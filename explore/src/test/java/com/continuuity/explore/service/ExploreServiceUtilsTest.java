package com.continuuity.explore.service;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 *
 */
public class ExploreServiceUtilsTest {

  @Test
  public void testHiveVersion() throws Exception {
    // This would throw an exception if it didn't pass
    ExploreServiceUtils.checkHiveSupportWithSecurity(new Configuration(), this.getClass().getClassLoader());
  }

}
