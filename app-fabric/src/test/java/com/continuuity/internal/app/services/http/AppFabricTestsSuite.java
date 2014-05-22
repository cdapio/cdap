package com.continuuity.internal.app.services.http;

import com.continuuity.common.conf.Constants;

import com.continuuity.internal.app.services.http.handlers.AppFabricHttpHandlerTest;
import com.continuuity.internal.app.services.http.handlers.PingHandlerTest;
import com.google.common.collect.ObjectArrays;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicHeader;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test Suite for running all API tests.
 */
@RunWith(value = Suite.class)
@Suite.SuiteClasses(value = {PingHandlerTest.class, AppFabricHttpHandlerTest.class})
public class AppFabricTestsSuite {
  private static final String API_KEY = "SampleTestApiKey";
  private static final Header AUTH_HEADER = new BasicHeader(Constants.Gateway.CONTINUUITY_API_KEY, API_KEY);

  public static HttpResponse doGet(String resource) throws Exception {
    return doGet(resource, null);
  }

  public static HttpResponse doGet(String resource, Header[] headers) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpGet get = new HttpGet(AppFabricTestBase.getEndPoint(resource));

    if (headers != null) {
      get.setHeaders(ObjectArrays.concat(AUTH_HEADER, headers));
    } else {

      get.setHeader(AUTH_HEADER);
    }
    return client.execute(get);
  }

  public static HttpResponse execute(HttpUriRequest request) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    request.setHeader(AUTH_HEADER);
    return client.execute(request);
  }

  public static HttpPost getPost(String resource) throws Exception {
    HttpPost post = new HttpPost(AppFabricTestBase.getEndPoint(resource));
    post.setHeader(AUTH_HEADER);
    return post;
  }

  public static HttpPut getPut(String resource) throws Exception {
    HttpPut put = new HttpPut(AppFabricTestBase.getEndPoint(resource));
    put.setHeader(AUTH_HEADER);
    return put;
  }
  public static HttpResponse doPost(String resource) throws Exception {
    return doPost(resource, null, null);
  }


  public static HttpResponse doPost(String resource, String body) throws Exception {
    return doPost(resource, body, null);
  }

  public static HttpResponse doPost(String resource, String body, Header[] headers) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpPost post = new HttpPost(AppFabricTestBase.getEndPoint(resource));

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

  public static HttpResponse doPost(HttpPost post) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    post.setHeader(AUTH_HEADER);
    return client.execute(post);
  }


  public static HttpResponse doPut(String resource) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpPut put = new HttpPut(AppFabricTestBase.getEndPoint(resource));
    put.setHeader(AUTH_HEADER);
    return doPut(resource, null);
  }

  public static HttpResponse doPut(String resource, String body) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpPut put = new HttpPut(AppFabricTestBase.getEndPoint(resource));
    if (body != null) {
      put.setEntity(new StringEntity(body));
    }
    put.setHeader(AUTH_HEADER);
    return client.execute(put);
  }

  public static HttpResponse doDelete(String resource) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpDelete delete = new HttpDelete(AppFabricTestBase.getEndPoint(resource));
    delete.setHeader(AUTH_HEADER);
    return client.execute(delete);
  }
}
