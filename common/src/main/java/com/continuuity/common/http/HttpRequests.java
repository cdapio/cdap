/*
 * Copyright 2012-2014 Continuuity, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.continuuity.common.http;

import com.google.common.io.ByteStreams;
import com.google.common.io.InputSupplier;
import com.google.gson.Gson;
import com.google.gson.JsonElement;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
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
    String requestMethod = request.getMethod().name();
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
        try {
          ByteStreams.copy(bodySrc, os);
        } finally {
          os.close();
        }
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
    return execute(HttpRequest.builder(httpMethod, url).build(), HttpRequestConfig.DEFAULT);
  }

  /**
   * Executes a GET request with the specified URL.
   *
   * @param url URL to hit
   * @return HTTP response
   * @throws IOException
   */
  public static HttpResponse get(URL url) throws IOException {
    return execute(HttpRequest.get(url).build(), HttpRequestConfig.DEFAULT);
  }

  /**
   * Executes a DELETE request with the specified URL.
   *
   * @param url URL to hit
   * @return HTTP response
   * @throws IOException
   */
  public static HttpResponse delete(URL url) throws IOException {
    return execute(HttpRequest.delete(url).build(), HttpRequestConfig.DEFAULT);
  }

  /**
   * Executes a POST request with the specified URL.
   *
   * @param url URL to hit
   * @return HTTP response
   * @throws IOException
   */
  public static HttpResponse post(URL url) throws IOException {
    return execute(HttpRequest.post(url).build(), HttpRequestConfig.DEFAULT);
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
    return execute(HttpRequest.post(url).withBody(body).build(), HttpRequestConfig.DEFAULT);
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
    return execute(HttpRequest.post(url).withBody(GSON.toJson(body)).build(), HttpRequestConfig.DEFAULT);
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
    return execute(HttpRequest.post(url).withBody(body).build(), HttpRequestConfig.DEFAULT);
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
    return execute(HttpRequest.post(url).withBody(body).build(), HttpRequestConfig.DEFAULT);
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
    return execute(HttpRequest.post(url).withBody(body).build(), HttpRequestConfig.DEFAULT);
  }

  private static boolean isSuccessful(int responseCode) {
    return 200 <= responseCode && responseCode < 300;
  }
}
