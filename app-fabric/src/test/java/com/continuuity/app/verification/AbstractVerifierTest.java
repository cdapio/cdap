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
    AbstractVerifier v = new AbstractVerifier<String>() {

      @Override
      protected String getName(String input) {
        return input;
      }
    };
    Assert.assertTrue(v.verify("foo").isSuccess());
    Assert.assertTrue(v.verify("mydataset").isSuccess());
    Assert.assertFalse(v.verify("foo name").isSuccess());
    Assert.assertTrue(v.verify("foo-name").isSuccess());
    Assert.assertTrue(v.verify("foo_name").isSuccess());
    Assert.assertTrue(v.verify("foo1234").isSuccess());
    Assert.assertFalse(v.verify("foo^ name").isSuccess());
    Assert.assertFalse(v.verify("foo^name").isSuccess());
    Assert.assertFalse(v.verify("foo/name").isSuccess());
    Assert.assertFalse(v.verify("foo$name").isSuccess());
  }

}
