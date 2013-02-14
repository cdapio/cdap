/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.verification;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests AbstractVerifier class at function level
 */
public class AbstractVerifierTest {

  /**
   * Checking if name is an id or no.
   */
  @Test
  public void testId() throws Exception {
    AbstractVerifier v = new AbstractVerifier() {
      @Override
      protected boolean isId(String name) {
        return super.isId(name);
      }
    };
    Assert.assertTrue(v.isId("foo"));
    Assert.assertTrue(v.isId("mydataset"));
    Assert.assertFalse(v.isId("foo name"));
    Assert.assertTrue(v.isId("foo-name"));
    Assert.assertTrue(v.isId("foo_name"));
    Assert.assertTrue(v.isId("foo1234"));
    Assert.assertFalse(v.isId("foo^ name"));
    Assert.assertFalse(v.isId("foo^name"));
    Assert.assertFalse(v.isId("foo/name"));
    Assert.assertFalse(v.isId("foo$name"));
  }

}
