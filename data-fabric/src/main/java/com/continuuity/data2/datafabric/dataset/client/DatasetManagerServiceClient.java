package com.continuuity.data2.datafabric.dataset.client;

import com.continuuity.data2.datafabric.dataset.service.DatasetInstanceMeta;
import com.continuuity.data2.dataset2.manager.InstanceConflictException;
import com.continuuity.data2.dataset2.manager.ModuleConflictException;
import com.continuuity.internal.data.dataset.DatasetInstanceProperties;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.discovery.TimeLimitEndpointStrategy;
import com.continuuity.data2.datafabric.dataset.type.DatasetTypeMeta;
import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;
import org.apache.twill.discovery.DiscoveryServiceClient;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.twill.filesystem.Location;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Provides programmatic APIs to access {@link com.continuuity.data2.datafabric.dataset.service.DatasetManagerService}.
 * Just a java wrapper for accessing service's REST API.
 */
public class DatasetManagerServiceClient {
  private static final Gson GSON = new Gson();

  private EndpointStrategy endpointStrategy;

  @Inject
  public DatasetManagerServiceClient(DiscoveryServiceClient discoveryClient) {

    this.endpointStrategy = new TimeLimitEndpointStrategy(
      new RandomEndpointStrategy(discoveryClient.discover(Constants.Service.DATASET_MANAGER)),
      1L, TimeUnit.SECONDS);
  }

  public DatasetInstanceMeta getInstance(String instanceName) throws IOException {
    HttpResponse response = doGet("instances/" + instanceName);
    if (HttpResponseStatus.NOT_FOUND.getCode() == response.responseCode) {
      return null;
    }
    return GSON.fromJson(new String(response.responseBody, Charsets.UTF_8), DatasetInstanceMeta.class);
  }

  public DatasetTypeMeta getType(String typeName) throws IOException {
    HttpResponse response = doGet("types/" + typeName);
    if (HttpResponseStatus.NOT_FOUND.getCode() == response.responseCode) {
      return null;
    }
    return GSON.fromJson(new String(response.responseBody, Charsets.UTF_8), DatasetTypeMeta.class);
  }

  public void addInstance(String datasetInstanceName, String datasetType, DatasetInstanceProperties props)
    throws InstanceConflictException, IOException {

    HttpResponse response = doPost("instances/" + datasetInstanceName,
                                   GSON.toJson(props),
                                   ImmutableMap.of("type-name", datasetType));
    if (HttpResponseStatus.CONFLICT.getCode() == response.responseCode) {
      throw new InstanceConflictException("Failed to add instance " + datasetInstanceName +
                                            ", reason: " + getDetails(response));
    }
    if (HttpResponseStatus.OK.getCode() != response.responseCode) {
      throw new IOException("Failed to add instance " + datasetInstanceName + ", reason: " + getDetails(response));
    }
  }

  public void deleteInstance(String datasetInstanceName) throws InstanceConflictException, IOException {
    HttpResponse response = doDelete("instances/" + datasetInstanceName);
    if (HttpResponseStatus.CONFLICT.getCode() == response.responseCode) {
      throw new InstanceConflictException("Failed to delete instance " + datasetInstanceName +
                                            ", reason: " + getDetails(response));
    }
    if (HttpResponseStatus.OK.getCode() != response.responseCode) {
      throw new IOException("Failed to delete instance " + datasetInstanceName + ", reason: " + getDetails(response));
    }
  }

  public void addModule(String moduleName, String className, Location jarLocation)
    throws ModuleConflictException, IOException {
    HttpResponse response = doPost("modules/" + moduleName,
                                   jarLocation,
                                   ImmutableMap.of("class-name", className));

    if (HttpResponseStatus.CONFLICT.getCode() == response.responseCode) {
      throw new ModuleConflictException("Failed to add module " + moduleName + ", reason: " + getDetails(response));
    }
    if (HttpResponseStatus.OK.getCode() != response.responseCode) {
      throw new IOException("Failed to add module " + moduleName + ", reason: " + getDetails(response));
    }
  }


  public void deleteModule(String moduleName) throws ModuleConflictException, IOException {
    HttpResponse response = doDelete("modules/" + moduleName);

    if (HttpResponseStatus.CONFLICT.getCode() == response.responseCode) {
      throw new ModuleConflictException("Failed to delete module " + moduleName +
                                          ", reason: " + getDetails(response));
    }
    if (HttpResponseStatus.OK.getCode() != response.responseCode) {
      throw new IOException("Failed to delete module " + moduleName + ", reason: " + getDetails(response));
    }
  }

  private HttpResponse doGet(String resource) throws IOException {
    return doRequest(resource, "GET", null, null, null);
  }

  private HttpResponse doPost(String resource, String body, Map<String, String> headers) throws IOException {
    return doRequest(resource, "POST", headers, body, null);
  }

  private HttpResponse doPost(String resource, Location bodySrc, Map<String, String> headers) throws IOException {
    InputStream is = bodySrc.getInputStream();
    try {
      return doRequest(resource, "POST", headers, null, is);
    } finally {
      is.close();
    }
  }

  private HttpResponse doDelete(String resource) throws IOException {
    return doRequest(resource, "DELETE", null, null, null);
  }

  private DatasetManagerServiceClient.HttpResponse doRequest(String resource, String requestMethod,
                                                             @Nullable Map<String, String> headers,
                                                             @Nullable String body,
                                                             @Nullable InputStream bodySrc) throws IOException {
    Preconditions.checkArgument(!(body != null && bodySrc != null), "only one of body and bodySrc can be used as body");

    URL url = new URL(resolve(resource));
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
      return getResponse(conn);
    } finally {
      conn.disconnect();
    }
  }

  private HttpResponse getResponse(HttpURLConnection conn) throws IOException {
    byte[] body = null;
    if (conn.getDoInput()) {
      InputStream is = conn.getInputStream();
      try {
        body = ByteStreams.toByteArray(is);
      } finally {
        is.close();
      }
    }
    return new HttpResponse(conn.getResponseCode(), conn.getResponseMessage(), body);
  }

  private String getDetails(HttpResponse response) throws IOException {
    return String.format("Response code: %s, message:'%s', body: '%s'",
                         response.responseCode, response.responseMessage,
                         new String(response.responseBody, Charsets.UTF_8));

  }

  private String resolve(String resource) {
    InetSocketAddress addr = this.endpointStrategy.pick().getSocketAddress();
    return String.format("http://%s:%s/%s/datasets/%s", addr.getHostName(), addr.getPort(),
                         Constants.Dataset.Manager.VERSION, resource);
  }

  private final class HttpResponse {
    private int responseCode;
    private String responseMessage;
    private byte[] responseBody;

    private HttpResponse(int responseCode, String responseMessage, byte[] responseBody) {
      this.responseCode = responseCode;
      this.responseMessage = responseMessage;
      this.responseBody = responseBody;
    }
  }
}
