package com.continuuity.explore.service;

import org.junit.Test;

/**
 *
 */
public class ExploreServiceUtilsTest {

  @Test
  public void testHiveVersion() throws Exception {
    // This would throw an exception if it didn't pass
    ExploreServiceUtils.checkHiveVersion(this.getClass().getClassLoader());
  }

}
