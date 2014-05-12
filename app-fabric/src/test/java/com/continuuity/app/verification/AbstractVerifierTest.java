/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.verification;

import com.continuuity.app.Id;
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
    AbstractVerifier<String> v = new AbstractVerifier<String>() {

      @Override
      protected String getName(String input) {
        return input;
      }
    };

    Id.Application appId = Id.Application.from("test", "some");

    Assert.assertTrue(v.verify(appId, "foo").isSuccess());
    Assert.assertTrue(v.verify(appId, "mydataset").isSuccess());
    Assert.assertFalse(v.verify(appId, "foo name").isSuccess());
    Assert.assertTrue(v.verify(appId, "foo-name").isSuccess());
    Assert.assertTrue(v.verify(appId, "foo_name").isSuccess());
    Assert.assertTrue(v.verify(appId, "foo1234").isSuccess());
    Assert.assertFalse(v.verify(appId, "foo^ name").isSuccess());
    Assert.assertFalse(v.verify(appId, "foo^name").isSuccess());
    Assert.assertFalse(v.verify(appId, "foo/name").isSuccess());
    Assert.assertFalse(v.verify(appId, "foo$name").isSuccess());
  }

}
