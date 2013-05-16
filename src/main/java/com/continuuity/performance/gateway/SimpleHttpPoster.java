package com.continuuity.performance.gateway;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

/**
 *
 */
public class SimpleHttpPoster implements HttpPoster {
  String url;
  Map<String, String> httpHeaders;
  private final HttpPost post;
  URI uri;

  public SimpleHttpPoster(String url, Map<String, String> httpHeaders) {
    this.url = url;
    this.httpHeaders = httpHeaders;

    post = new HttpPost(url);
    if (httpHeaders != null && !httpHeaders.isEmpty()) {
      for (Map.Entry<String, String> header : httpHeaders.entrySet()) {
        post.addHeader(header.getKey(), header.getValue());
      }
    }
  }

  @Override
  public void init() {
  }

  @Override
  public void post(byte[] message) throws Exception {
    post.setEntity(new ByteArrayEntity(message));
    HttpClient client = new DefaultHttpClient();

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

  @Override
  public void post(String message) throws Exception {
    post(message.getBytes());
  }
}
