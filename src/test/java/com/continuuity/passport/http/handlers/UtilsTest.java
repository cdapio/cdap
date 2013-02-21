/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.http.handlers;

import org.json.simple.JSONObject;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class UtilsTest {

  @Test
  public void testJSONObject() throws Exception {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("status", "good");
    jsonObject.put("message", "test");
    String s = jsonObject.toJSONString();
    Assert.assertNotNull(s);
  }

}
