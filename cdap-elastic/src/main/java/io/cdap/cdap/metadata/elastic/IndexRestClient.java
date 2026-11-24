/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.metadata.elastic;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import java.io.Closeable;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.ConnectException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A small JSON-over-HTTP client for the REST API of the search cluster that stores the metadata
 * index. It only depends on Apache HttpClient and Gson, and it uses request and response shapes
 * that are the same in OpenSearch and in Elasticsearch 6.8, so that the same client can read a
 * legacy Elasticsearch index during a migration.
 *
 * <p>The client does not interpret HTTP status codes: every response that the server sends is
 * returned to the caller, and only transport failures throw.
 */
public final class IndexRestClient implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(IndexRestClient.class);

  /** The content type of the body of a bulk request. */
  public static final ContentType NDJSON = ContentType.create("application/x-ndjson",
      StandardCharsets.UTF_8);

  private static final int DEFAULT_CONNECT_TIMEOUT_MS = 5000;
  private static final int DEFAULT_SOCKET_TIMEOUT_MS = 60000;
  private static final int MAX_CONNECTIONS_PER_HOST = 10;
  private static final int MAX_CONNECTIONS = 30;

  private final List<HttpHost> hosts;
  private final CloseableHttpClient httpClient;
  private final AtomicInteger nextHost = new AtomicInteger();

  /**
   * Creates a client.
   *
   * @param hostList comma-separated list of hosts, each of the form {@code [scheme://]host[:port]}
   * @param username the user for basic authentication, or null for no authentication
   * @param password the password for basic authentication, or null for no authentication
   * @param verifyTls whether to verify the server certificate of HTTPS connections
   */
  public IndexRestClient(String hostList, @Nullable String username, @Nullable String password,
      boolean verifyTls) {
    this(hostList, username, password, verifyTls, DEFAULT_SOCKET_TIMEOUT_MS);
  }

  /**
   * Creates a client with a specific socket timeout.
   *
   * @param hostList comma-separated list of hosts, each of the form {@code [scheme://]host[:port]}
   * @param username the user for basic authentication, or null for no authentication
   * @param password the password for basic authentication, or null for no authentication
   * @param verifyTls whether to verify the server certificate of HTTPS connections
   * @param socketTimeoutMs how long to wait for response data before the request fails
   */
  public IndexRestClient(String hostList, @Nullable String username, @Nullable String password,
      boolean verifyTls, int socketTimeoutMs) {
    this.hosts = parseHosts(hostList);
    HttpClientBuilder builder = HttpClients.custom()
        .setDefaultRequestConfig(RequestConfig.custom()
            .setConnectTimeout(DEFAULT_CONNECT_TIMEOUT_MS)
            .setConnectionRequestTimeout(DEFAULT_CONNECT_TIMEOUT_MS)
            .setSocketTimeout(socketTimeoutMs)
            .build())
        .setMaxConnPerRoute(MAX_CONNECTIONS_PER_HOST)
        .setMaxConnTotal(MAX_CONNECTIONS)
        .disableAutomaticRetries();
    if (username != null && password != null) {
      LOG.info("Creating REST client with authentication, username: {}", username);
      CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
      credentialsProvider.setCredentials(AuthScope.ANY,
          new UsernamePasswordCredentials(username, password));
      builder.setDefaultCredentialsProvider(credentialsProvider);
    }
    if (!verifyTls) {
      LOG.warn("Creating REST client with TLS verification DISABLED");
      try {
        SSLContext sslContext = SSLContextBuilder.create()
            .loadTrustMaterial((chain, authType) -> true).build();
        builder.setSSLContext(sslContext);
      } catch (NoSuchAlgorithmException | KeyManagementException | KeyStoreException e) {
        LOG.error("Unable to trust self-signed certificates", e);
      }
    }
    this.httpClient = builder.build();
  }

  /**
   * Parses a comma-separated list of hosts. The scheme defaults to http, and the port defaults to
   * 9200 for http and to 9243 for https.
   */
  static List<HttpHost> parseHosts(String hostList) {
    List<HttpHost> result = new ArrayList<>();
    for (String hostAndPort : hostList.split(",")) {
      String scheme = "http";
      String host = hostAndPort.trim();
      if (host.isEmpty()) {
        continue;
      }
      int schemeIdx = host.indexOf("://");
      if (schemeIdx > 0) {
        scheme = host.substring(0, schemeIdx);
        host = host.substring(schemeIdx + 3);
      }
      int port;
      int portIdx = host.lastIndexOf(":");
      if (portIdx > 0) {
        try {
          port = Integer.parseInt(host.substring(portIdx + 1));
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException("Invalid HTTP host: " + host, e);
        }
        host = host.substring(0, portIdx);
      } else {
        port = scheme.equals("https") ? 9243 : 9200;
      }
      result.add(new HttpHost(host, port, scheme));
    }
    if (result.isEmpty()) {
      throw new IllegalArgumentException("No HTTP host found in '" + hostList + "'");
    }
    return Collections.unmodifiableList(result);
  }

  /**
   * Sends a request without parameters and without a body.
   *
   * @param method the HTTP method
   * @param pathSegments the segments of the request path. Each segment is URL-encoded
   * @return the response of the server, whatever its status code is
   * @throws IOException if no response was received
   */
  public Response execute(String method, String... pathSegments) throws IOException {
    return execute(method, Arrays.asList(pathSegments), Collections.emptyMap(), null);
  }

  /**
   * Sends a request with a JSON body.
   *
   * @param method the HTTP method
   * @param pathSegments the segments of the request path. Each segment is URL-encoded
   * @param parameters the query parameters
   * @param jsonBody the request body, or null to send no body
   * @return the response of the server, whatever its status code is
   * @throws IOException if no response was received
   */
  public Response execute(String method, List<String> pathSegments, Map<String, String> parameters,
      @Nullable JsonElement jsonBody) throws IOException {
    return execute(method, pathSegments, parameters,
        jsonBody == null ? null : jsonBody.toString(), ContentType.APPLICATION_JSON);
  }

  /**
   * Sends a request with a body of any content type.
   *
   * @param method the HTTP method
   * @param pathSegments the segments of the request path. Each segment is URL-encoded
   * @param parameters the query parameters
   * @param body the request body, or null to send no body
   * @param contentType the content type of the body
   * @return the response of the server, whatever its status code is
   * @throws IOException if no response was received
   */
  public Response execute(String method, List<String> pathSegments, Map<String, String> parameters,
      @Nullable String body, ContentType contentType) throws IOException {
    int first = Math.floorMod(nextHost.getAndIncrement(), hosts.size());
    IOException failure = null;
    // Only try another host if the connection could not be established. After that point the
    // request may have reached the server, and it must not be sent twice.
    for (int i = 0; i < hosts.size(); i++) {
      HttpHost host = hosts.get((first + i) % hosts.size());
      try {
        return execute(host, method, pathSegments, parameters, body, contentType);
      } catch (ConnectException | ConnectTimeoutException | UnknownHostException e) {
        LOG.debug("Unable to connect to {}", host, e);
        if (failure != null) {
          e.addSuppressed(failure);
        }
        failure = e;
      }
    }
    throw failure;
  }

  private Response execute(HttpHost host, String method, List<String> pathSegments,
      Map<String, String> parameters, @Nullable String body, ContentType contentType)
      throws IOException {
    String path = pathSegments.stream().map(IndexRestClient::encode)
        .collect(Collectors.joining("/"));
    String query = parameters.entrySet().stream()
        .map(e -> encode(e.getKey()) + "=" + encode(e.getValue()))
        .collect(Collectors.joining("&"));
    URI uri = URI.create(host.toURI() + "/" + path + (query.isEmpty() ? "" : "?" + query));
    Request request = new Request(method, uri);
    if (body != null) {
      request.setEntity(new StringEntity(body, contentType));
    }
    try (CloseableHttpResponse response = httpClient.execute(request)) {
      HttpEntity entity = response.getEntity();
      return new Response(method, uri, response.getStatusLine().getStatusCode(),
          entity == null ? "" : EntityUtils.toString(entity, StandardCharsets.UTF_8));
    }
  }

  /**
   * Encodes a path segment or a query parameter, so that all reserved characters are escaped,
   * including the slash.
   */
  private static String encode(String value) {
    try {
      // URLEncoder encodes for HTML forms: it writes a space as a plus sign.
      return URLEncoder.encode(value, StandardCharsets.UTF_8.name()).replace("+", "%20");
    } catch (UnsupportedEncodingException e) {
      // Every Java platform has UTF-8.
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void close() throws IOException {
    httpClient.close();
  }

  /**
   * An HTTP request with any method and an optional body. HttpClient has no DELETE request with
   * a body, which the clear-scroll API needs.
   */
  private static final class Request extends HttpEntityEnclosingRequestBase {

    private final String method;

    Request(String method, URI uri) {
      this.method = method;
      setURI(uri);
    }

    @Override
    public String getMethod() {
      return method;
    }
  }

  /**
   * The response to a request.
   */
  public static final class Response {

    private final String method;
    private final URI uri;
    private final int status;
    private final String body;

    Response(String method, URI uri, int status, String body) {
      this.method = method;
      this.uri = uri;
      this.status = status;
      this.body = body;
    }

    public int getStatus() {
      return status;
    }

    public String getBody() {
      return body;
    }

    /** Returns whether the status code is 2xx. */
    public boolean isSuccess() {
      return status >= 200 && status < 300;
    }

    /**
     * Parses the body as a JSON object.
     *
     * @throws IOException if the body is not a JSON object
     */
    public JsonObject getJson() throws IOException {
      try {
        JsonElement element = new JsonParser().parse(body);
        if (element != null && element.isJsonObject()) {
          return element.getAsJsonObject();
        }
      } catch (JsonSyntaxException e) {
        throw new IOException("Response is not valid JSON: " + this, e);
      }
      throw new IOException("Response is not a JSON object: " + this);
    }

    /**
     * Returns this response if its status code is 2xx.
     *
     * @throws IOException if the status code is not 2xx
     */
    public Response ensureSuccess() throws IOException {
      if (!isSuccess()) {
        throw new IOException("Request failed: " + this);
      }
      return this;
    }

    @Override
    public String toString() {
      return method + " " + uri + " returned HTTP " + status + ": "
          + (body.length() > 2000 ? body.substring(0, 2000) + "..." : body);
    }
  }
}
