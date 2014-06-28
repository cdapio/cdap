/*
 * Copyright Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.http;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Simple HTTP client that uses {@link HttpURLConnection}.
 */
public final class HttpRequests {

  private HttpRequests() {}

  /**
   * Executes a GET request to the url provided.
   * @param url URL of the request.
   * @return response of the request
   * @throws IOException
   */
  public static HttpResponse get(URL url) throws IOException {
    return doRequest("GET", url, null, (byte[]) null, null);
  }

  /**
   * Executes a GET request to the url provided.
   * @param url URL of the request.
   * @return response of the request
   * @throws IOException
   */
  public static HttpResponse put(URL url) throws IOException {
    return doRequest("GET", url, null, (byte[]) null, null);
  }

  /**
   * Executes a POST request to the url provided.
   * @param url URL of the request.
   * @return response of the request
   * @throws IOException
   */
  public static HttpResponse post(URL url) throws IOException {
    return doRequest("POST", url, null, (byte[]) null, null);
  }

  /**
   * Executes a DELETE request to the url provided.
   * @param url URL of the request.
   * @return response of the request
   * @throws IOException
   */
  public static HttpResponse delete(URL url) throws IOException {
    return doRequest("DELETE", url, null, (byte[]) null, null);
  }

  /**
   * Executes a DELETE request to the url provided.
   * @param url URL of the request.
   * @param body Body of the request.
   * @param headers Headers of the request.
   * @return response of the request
   * @throws IOException
   */
  public static HttpResponse post(URL url, @Nullable String body, Map<String, String> headers) throws IOException {
    return doRequest("POST", url, headers, body != null ? body.getBytes(Charsets.UTF_8) : null, null);
  }

  /**
   * Same as {@link #post(java.net.URL, String, java.util.Map)}, accepts headers as string params for convenience
   */
  public static HttpResponse post(URL url, @Nullable String body,
                                  String headerName, String headerValue) throws IOException {

    return doRequest("POST", url, ImmutableMap.of(headerName, headerValue),
                     body != null ? body.getBytes(Charsets.UTF_8) : null, null);
  }

  /**
   * Same as {@link #post(java.net.URL, String, java.util.Map)}, accepts headers as string params for convenience
   */
  public static HttpResponse post(URL url, @Nullable String body,
                                  String header1Name, String header1Value,
                                  String header2Name, String header2Value) throws IOException {
    return doRequest("POST", url, ImmutableMap.of(header1Name, header1Value, header2Name, header2Value),
                     body != null ? body.getBytes(Charsets.UTF_8) : null, null);
  }

  /**
   * Executes a PUT request to the url provided.
   * @param url URL of the request.
   * @param body Body of the request.
   * @param headers Headers of the request.
   * @return response of the request
   * @throws IOException
   */
  public static HttpResponse put(URL url, @Nullable  String body, Map<String, String> headers) throws IOException {
    return doRequest("PUT", url, headers, body != null ? body.getBytes(Charsets.UTF_8) : null, null);
  }

  /**
   * Same as {@link #put(java.net.URL, String, java.util.Map)} with no extra headers
   */
  public static HttpResponse put(URL url, @Nullable  String body) throws IOException {
    return doRequest("PUT", url, null, body != null ? body.getBytes(Charsets.UTF_8) : null, null);
  }

  /**
   * Same as {@link #put(java.net.URL, String, java.util.Map)}, accepts headers as string params for convenience
   */
  public static HttpResponse put(URL url, @Nullable String body,
                                 String headerName, String headerValue) throws IOException {

    return doRequest("PUT", url, ImmutableMap.of(headerName, headerValue),
                     body != null ? body.getBytes(Charsets.UTF_8) : null, null);
  }

  /**
   * Same as {@link #put(java.net.URL, String, java.util.Map)}, accepts headers as string params for convenience
   */
  public static HttpResponse put(URL url, @Nullable String body,
                                  String header1Name, String header1Value,
                                  String header2Name, String header2Value) throws IOException {

    return doRequest("PUT", url, ImmutableMap.of(header1Name, header1Value, header2Name, header2Value),
                     body != null ? body.getBytes(Charsets.UTF_8) : null, null);
  }

  /**
   * Executes an HTTP request to the url provided.
   * @param requestMethod HTTP method of the request.
   * @param url URL of the request.
   * @param headers Headers of the request.
   * @param body Body of the request. If provided, bodySrc must be null.
   * @param bodySrc Body of the request as an {@link InputStream}. If provided, body must be null.
   * @return repsonse of the request
   * @throws IOException
   */
  public static HttpResponse doRequest(String requestMethod, URL url,
                                 @Nullable Map<String, String> headers,
                                 @Nullable byte[] body,
                                 @Nullable InputStream bodySrc) throws IOException {

    Preconditions.checkArgument(!(body != null && bodySrc != null), "only one of body and bodySrc can be used as body");

    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod(requestMethod);

    if (headers != null) {
      for (Map.Entry<String, String> header : headers.entrySet()) {
        conn.setRequestProperty(header.getKey(), header.getValue());
      }
    }

    if (body != null || bodySrc != null) {
      conn.setDoOutput(true);
    }

    conn.connect();
    try {
      if (body != null || bodySrc != null) {
        OutputStream os = conn.getOutputStream();
        if (body != null) {
          os.write(body);
        } else {
          ByteStreams.copy(bodySrc, os);
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
   * Executes an HTTP request to the url provided.
   * @param requestMethod HTTP method of the request.
   * @param url URL of the request.
   * @param headers Headers of the request.
   * @param body Body of the request. If provided, bodySrc must be null.
   * @param bodySrc Body of the request as an {@link InputStream}. If provided, body must be null.
   * @return repsonse of the request
   * @throws IOException
   */
  public static HttpResponse doRequest(String requestMethod, URL url,
                                       @Nullable Map<String, String> headers,
                                       @Nullable String body,
                                       @Nullable InputStream bodySrc) throws IOException {
    return doRequest(requestMethod, url, headers, body != null ? body.getBytes(Charsets.UTF_8) : null, bodySrc);
  }

  private static boolean isSuccessful(int responseCode) {
    return 200 <= responseCode && responseCode < 300;
  }

}
