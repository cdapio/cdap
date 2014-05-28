package com.continuuity.internal.app.services.http.handlers;

import com.continuuity.internal.app.services.http.AppFabricTestBase;
import org.apache.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test ping handler.
 */
public class PingHandlerTest extends AppFabricTestBase {
  @Test
  public void testPing() throws Exception {
    HttpResponse response = doGet("/ping");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  @Test
  public void testStatus() throws Exception {
    HttpResponse response = doGet("/status");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

}
