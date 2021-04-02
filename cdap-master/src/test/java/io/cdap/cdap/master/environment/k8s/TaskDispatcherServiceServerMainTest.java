package io.cdap.cdap.master.environment.k8s;

import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

import java.net.HttpURLConnection;
import java.net.URL;

public class TaskDispatcherServiceServerMainTest extends MasterServiceMainTestBase {

  @Test
  public void testDispatcherService() throws Exception {
    URL url = getTaskDispatcherBaseURI().resolve("/v3Internal/dispatcher/get").toURL();
    HttpResponse response = HttpRequests.execute(HttpRequest.get(url).build(), new DefaultHttpRequestConfig(false));
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
  }
}


