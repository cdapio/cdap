package com.continuuity.gateway;

import com.continuuity.gateway.handlers.log.LogHandlerTest;
import com.continuuity.gateway.handlers.metrics.MetricsSuiteTestBase;
import com.continuuity.gateway.handlers.metrics.MetricsDeleteTest;
import com.continuuity.gateway.handlers.metrics.MetricsDiscoveryQueryTest;
import com.continuuity.gateway.handlers.metrics.MetricsQueryTest;
import com.google.common.collect.ObjectArrays;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test Suite for running all API tests.
 */
@RunWith(value = Suite.class)
@Suite.SuiteClasses(value = {
  MetricsQueryTest.class,
  MetricsDeleteTest.class,
  MetricsDiscoveryQueryTest.class,
  LogHandlerTest.class
})

public class MetricsServiceTestsSuite  {

  private static final Header AUTH_HEADER = MetricsSuiteTestBase.getAuthHeader();

  public static HttpResponse doGet(String resource) throws Exception {
    return doGet(resource, null);
  }

  public static HttpResponse doGet(String resource, Header[] headers) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpGet get = new HttpGet(MetricsSuiteTestBase.getEndPoint(resource));
    if (headers != null) {
      get.setHeaders(ObjectArrays.concat(AUTH_HEADER, headers));
    } else {
      get.setHeader(AUTH_HEADER);
    }
    return client.execute(get);
  }

  public static HttpResponse doPut(String resource) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpPut put = new HttpPut(MetricsSuiteTestBase.getEndPoint(resource));
    put.setHeader(AUTH_HEADER);
    return client.execute(put);
  }

  public static HttpResponse doPut(String resource, String body) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpPut put = new HttpPut(MetricsSuiteTestBase.getEndPoint(resource));
    if (body != null) {
      put.setEntity(new StringEntity(body));
    }
    put.setHeader(AUTH_HEADER);
    return client.execute(put);
  }

  public static HttpResponse doPost(HttpPost post) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    post.setHeader(AUTH_HEADER);
    return client.execute(post);
  }

  public static HttpPost getPost(String resource) throws Exception {
    HttpPost post = new HttpPost(MetricsSuiteTestBase.getEndPoint(resource));
    post.setHeader(AUTH_HEADER);
    return post;
  }

  public static HttpResponse doPost(String resource, String body) throws Exception {
    return doPost(resource, body, null);
  }

  public static HttpResponse doPost(String resource, String body, Header[] headers) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpPost post = new HttpPost(MetricsSuiteTestBase.getEndPoint(resource));
    if (body != null) {
      post.setEntity(new StringEntity(body));
    }

    if (headers != null) {
      post.setHeaders(ObjectArrays.concat(AUTH_HEADER, headers));
    } else {
      post.setHeader(AUTH_HEADER);
    }
    return client.execute(post);
  }

  public static HttpResponse doDelete(String resource) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpDelete delete = new HttpDelete(MetricsSuiteTestBase.getEndPoint(resource));
    delete.setHeader(AUTH_HEADER);
    return client.execute(delete);
  }

}
