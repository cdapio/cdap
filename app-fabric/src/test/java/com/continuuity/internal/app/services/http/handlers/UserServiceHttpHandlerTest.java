package com.continuuity.internal.app.services.http.handlers;

import com.continuuity.internal.app.services.http.AppFabricTestsSuite;
import org.apache.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class UserServiceHttpHandlerTest {
  @Test
  public void testStart() throws Exception {
    HttpResponse response = AppFabricTestsSuite.doPost("/v2/user/test/start");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

}
