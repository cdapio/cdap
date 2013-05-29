package com.continuuity.performance.gateway;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.IOException;
import java.util.Map;

/**
 * HttpClient for sending HttpPosts to Gateway.
 */
public final class SimpleHttpClient {
  private final HttpPost post;

  public SimpleHttpClient(String url, Map<String, String> httpHeaders) {
    post = new HttpPost(url);
    if (httpHeaders != null && !httpHeaders.isEmpty()) {
      for (Map.Entry<String, String> header : httpHeaders.entrySet()) {
        post.addHeader(header.getKey(), header.getValue());
      }
    }
  }

  public void post(byte[] message) throws Exception {
    post.setEntity(new ByteArrayEntity(message));
    org.apache.http.client.HttpClient client = new DefaultHttpClient();

    try {
      HttpResponse response = client.execute(post);
      if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
        throw new Exception("Unexpected HTTP response: " + response.getStatusLine());
      }
      client.getConnectionManager().shutdown();
    } catch (IOException e) {
      throw new Exception("Error sending HTTP request: " + e.getMessage(), e);
    }
  }
}