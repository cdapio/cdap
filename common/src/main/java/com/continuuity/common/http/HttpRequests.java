/*
 * Copyright (c) 2012-2014 Continuuity Inc. All rights reserved.
 */
package com.continuuity.common.http;

import com.google.common.io.ByteStreams;
import com.google.common.io.InputSupplier;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

/**
 * Executes {@link HttpRequest}s and returns an {@link HttpResponse}.
 */
public class HttpRequests {

  private static final Gson GSON = new Gson();

  /**
   * Executes an HTTP request to the url provided.
   *
   * @param request HTTP request to execute
   * @param requestConfig configuration for the HTTP request to execute
   * @return HTTP response
   */
  public static HttpResponse execute(HttpRequest request, HttpRequestConfig requestConfig) throws IOException {
    String requestMethod = request.getMethod().getMethodName();
    URL url = request.getURL();

    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod(requestMethod);
    conn.setReadTimeout(requestConfig.getReadTimeout());
    conn.setConnectTimeout(requestConfig.getConnectTimeout());

    Map<String, String> headers = request.getHeaders();
    if (headers != null) {
      for (Map.Entry<String, String> header : headers.entrySet()) {
        conn.setRequestProperty(header.getKey(), header.getValue());
      }
    }

    InputSupplier<? extends InputStream> bodySrc = request.getBody();
    if (bodySrc != null) {
      conn.setDoOutput(true);
    }

    conn.connect();

    try {
      if (bodySrc != null) {
        OutputStream os = conn.getOutputStream();
        ByteStreams.copy(bodySrc, os);
      }

      try {
        if (isSuccessful(conn.getResponseCode())) {
          return new HttpResponse(conn.getResponseCode(), conn.getResponseMessage(),
                                  ByteStreams.toByteArray(conn.getInputStream()));
        }
      } catch (FileNotFoundException e) {
        // Server returns 404. Hence handle as error flow below. Intentional having empty catch block.
      }

      // Non 2xx response
      InputStream es = conn.getErrorStream();
      byte[] content = (es == null) ? new byte[0] : ByteStreams.toByteArray(es);
      return new HttpResponse(conn.getResponseCode(), conn.getResponseMessage(), content);
    } finally {
      conn.disconnect();
    }
  }

  /**
   * Executes an HTTP request with default request configuration.
   *
   * @param request HTTP request to execute
   * @return HTTP response
   * @throws IOException
   */
  public static HttpResponse execute(HttpRequest request) throws IOException {
    return execute(request, HttpRequestConfig.DEFAULT);
  }

  /**
   * Executes an HTTP request with the specified HTTP method, URL, and default request configuration.
   *
   * @param httpMethod HTTP method to use
   * @param url URL to hit
   * @return HTTP response
   * @throws IOException
   */
  public static HttpResponse execute(HttpMethod httpMethod, URL url) throws IOException {
    return execute(new HttpRequest(httpMethod, url), HttpRequestConfig.DEFAULT);
  }

  /**
   * Executes an HTTP request with the specified HTTP method, URL, headers, and default request configuration.
   *
   * @param httpMethod HTTP method to use
   * @param url URL to hit
   * @param headers headers to include in the request
   * @return HTTP response
   * @throws IOException
   */
  public static HttpResponse execute(HttpMethod httpMethod, URL url, Map<String, String> headers) throws IOException {
    return execute(new HttpRequest(httpMethod, url, headers), HttpRequestConfig.DEFAULT);
  }

  /**
   * Executes an HTTP request with the specified HTTP method, URL, headers, body, and default request configuration.
   *
   * @param httpMethod HTTP method to use
   * @param url URL to hit
   * @param headers headers to include in the request
   * @param body body of the HTTP request
   * @return
   * @throws IOException
   */
  public static HttpResponse execute(HttpMethod httpMethod, URL url, Map<String, String> headers,
                                     String body) throws IOException {
    return execute(new HttpRequest(httpMethod, url, headers, body), HttpRequestConfig.DEFAULT);
  }

  /**
   * Executes an HTTP request with the specified HTTP method, URL, and default request configuration.
   *
   * @param httpMethod HTTP method to use
   * @param url URL to hit
   * @param requestConfig configuration for the HTTP request to execute
   * @return HTTP response
   * @throws IOException
   */
  public static HttpResponse execute(HttpMethod httpMethod, URL url,
                                     HttpRequestConfig requestConfig) throws IOException {
    return execute(new HttpRequest(httpMethod, url), requestConfig);
  }

  /**
   * Executes a GET request with the specified URL.
   *
   * @param url URL to hit
   * @return HTTP response
   * @throws IOException
   */
  public static HttpResponse get(URL url) throws IOException {
    return execute(new HttpRequest(HttpMethod.GET, url), HttpRequestConfig.DEFAULT);
  }

  /**
   * Executes a DELETE request with the specified URL.
   *
   * @param url URL to hit
   * @return HTTP response
   * @throws IOException
   */
  public static HttpResponse delete(URL url) throws IOException {
    return execute(new HttpRequest(HttpMethod.DELETE, url), HttpRequestConfig.DEFAULT);
  }

  /**
   * Executes a POST request with the specified URL.
   *
   * @param url URL to hit
   * @return HTTP response
   * @throws IOException
   */
  public static HttpResponse post(URL url) throws IOException {
    return execute(new HttpRequest(HttpMethod.POST, url), HttpRequestConfig.DEFAULT);
  }

  /**
   * Executes a POST request with the specified URL and body.
   *
   * @param url URL to hit
   * @param body body of the HTTP request
   * @return HTTP response
   * @throws IOException
   */
  public static HttpResponse post(URL url, String body) throws IOException {
    return execute(new HttpRequest(HttpMethod.POST, url, body), HttpRequestConfig.DEFAULT);
  }

  /**
   * Executes a POST request with the specified URL and body.
   *
   * @param url URL to hit
   * @param body body of the HTTP request
   * @return HTTP response
   * @throws IOException
   */
  public static HttpResponse post(URL url, JsonElement body) throws IOException {
    return execute(new HttpRequest(HttpMethod.POST, url, GSON.toJson(body)), HttpRequestConfig.DEFAULT);
  }

  /**
   * Executes a POST request with the specified URL and body.
   *
   * @param url URL to hit
   * @param body body of the HTTP request
   * @return HTTP response
   * @throws IOException
   */
  public static HttpResponse post(URL url, File body) throws IOException {
    return execute(new HttpRequest(HttpMethod.POST, url, body), HttpRequestConfig.DEFAULT);
  }

  /**
   * Executes a POST request with the specified URL and body.
   *
   * @param url URL to hit
   * @param body body of the HTTP request
   * @return HTTP response
   * @throws IOException
   */
  public static HttpResponse post(URL url, InputSupplier<? extends InputStream> body) throws IOException {
    return execute(new HttpRequest(HttpMethod.POST, url, body), HttpRequestConfig.DEFAULT);
  }

  /**
   * Executes a PUT request with the specified URL and body.
   *
   * @param url URL to hit
   * @param body body of the HTTP request
   * @return HTTP response
   * @throws IOException
   */
  public static HttpResponse put(URL url, String body) throws IOException {
    return execute(new HttpRequest(HttpMethod.PUT, url, body), HttpRequestConfig.DEFAULT);
  }

  private static boolean isSuccessful(int responseCode) {
    return 200 <= responseCode && responseCode < 300;
  }
}
