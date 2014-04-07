package com.continuuity.internal.app.services.http;

//import com.continuuity.internal.app.services.http.AppFabricTestsSuite;
import org.apache.http.HttpResponse;
  import org.junit.Assert;
import org.junit.Test;

/**
 * Test ping handler.
 */
public class PingHandlerTest {
  @Test
  public void testPing() throws Exception {
    //TestHelper.deployApplication(WordCountApp.class);
    HttpResponse response = AppFabricTestsSuite.doGet("/v2/ping");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

}
