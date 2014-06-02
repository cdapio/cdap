package com.continuuity.common.http;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;
import com.google.common.io.InputSupplier;

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
    return doRequest("GET", url, null, null, null);
  }

  /**
   * Executes a POST request to the url provided.
   * @param url URL of the request.
   * @return response of the request
   * @throws IOException
   */
  public static HttpResponse post(URL url) throws IOException {
    return doRequest("POST", url, null, null, null);
  }

  /**
   * Executes a POST request to the url provided.
   * @param url URL of the request.
   * @param body Body of the request.
   * @param headers Headers of the request.
   * @return response of the request
   * @throws IOException
   */
  public static HttpResponse post(URL url, String body, Map<String, String> headers) throws IOException {
    return doRequest("POST", url, headers, body, null);
  }

  /**
   * Executes a DELETE request to the url provided.
   * @param url URL of the request.
   * @return response of the request
   * @throws IOException
   */
  public static HttpResponse delete(URL url) throws IOException {
    return doRequest("DELETE", url, null, null, null);
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
                                 @Nullable InputSupplier<? extends InputStream> bodySrc) throws IOException {

    Preconditions.checkArgument(!(body != null && bodySrc != null), "only one of body and bodySrc can be used as body");

    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod(requestMethod);

    if (headers != null) {
      for (Map.Entry<String, String> header : headers.entrySet()) {
        conn.setRequestProperty(header.getKey(), header.getValue());
      }
    }

    conn.setDoInput(true);

    if (body != null || bodySrc != null) {
      conn.setDoOutput(true);
    }

    conn.connect();
    try {
      if (body != null || bodySrc != null) {
        OutputStream os = conn.getOutputStream();
        try {
          if (body != null) {
            os.write(body.getBytes(Charsets.UTF_8));
          } else {
            ByteStreams.copy(bodySrc, os);
          }
        } finally {
          os.close();
        }
      }
      byte[] responseBody = null;
      if (conn.getDoInput()) {
        InputStream is = conn.getInputStream();
        try {
          responseBody = ByteStreams.toByteArray(is);
        } finally {
          is.close();
        }
      }
      return new HttpResponse(conn.getResponseCode(), conn.getResponseMessage(), responseBody);
    } catch (FileNotFoundException e) {
      return new HttpResponse(404, conn.getResponseMessage(), null);
    } finally {
      conn.disconnect();
    }
  }

  /**
   * Represents the response from an HTTP request.
   */
  public static final class HttpResponse {
    private final int responseCode;
    private final String responseMessage;
    private final byte[] responseBody;

    private HttpResponse(int responseCode, String responseMessage, byte[] responseBody) {
      this.responseCode = responseCode;
      this.responseMessage = responseMessage;
      this.responseBody = responseBody;
    }

    public int getResponseCode() {
      return responseCode;
    }

    public String getResponseMessage() {
      return responseMessage;
    }

    public byte[] getResponseBody() {
      return responseBody;
    }
  }

}
