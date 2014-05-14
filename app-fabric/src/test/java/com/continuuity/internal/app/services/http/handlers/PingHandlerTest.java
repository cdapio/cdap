package com.continuuity.internal.app.services.http.handlers;

import com.continuuity.internal.app.services.http.AppFabricTestService;
import org.apache.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test ping handler.
 */
public class PingHandlerTest extends AppFabricTestService {
  @Test
  public void testPing() throws Exception {
    HttpResponse response = AppFabricTestService.doGet("/ping");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  @Test
  public void testStatus() throws Exception {
    HttpResponse response = AppFabricTestService.doGet("/status");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

}
