package com.continuuity.data2.datafabric.dataset.client;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.discovery.TimeLimitEndpointStrategy;
import com.continuuity.data2.datafabric.dataset.service.DatasetInstanceMeta;
import com.continuuity.data2.datafabric.dataset.type.DatasetTypeMeta;
import com.continuuity.data2.dataset2.manager.DatasetManagementException;
import com.continuuity.data2.dataset2.manager.InstanceConflictException;
import com.continuuity.data2.dataset2.manager.ModuleConflictException;
import com.continuuity.internal.data.dataset.DatasetInstanceProperties;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

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

  public DatasetInstanceMeta getInstance(String instanceName) throws DatasetManagementException {
    HttpResponse response = doGet("instances/" + instanceName);
    if (HttpResponseStatus.NOT_FOUND.getCode() == response.responseCode) {
      return null;
    }
    if (HttpResponseStatus.OK.getCode() != response.responseCode) {
      throw new DatasetManagementException(String.format("Cannot retrieve dataset instance %s info, details: %s",
                                                         instanceName, getDetails(response)));
    }

    return GSON.fromJson(new String(response.responseBody, Charsets.UTF_8), DatasetInstanceMeta.class);
  }

  public DatasetTypeMeta getType(String typeName) throws DatasetManagementException {
    HttpResponse response = doGet("types/" + typeName);
    if (HttpResponseStatus.NOT_FOUND.getCode() == response.responseCode) {
      return null;
    }
    if (HttpResponseStatus.OK.getCode() != response.responseCode) {
      throw new DatasetManagementException(String.format("Cannot retrieve dataset type %s info, details: %s",
                                                         typeName, getDetails(response)));
    }
    return GSON.fromJson(new String(response.responseBody, Charsets.UTF_8), DatasetTypeMeta.class);
  }

  public void addInstance(String datasetInstanceName, String datasetType, DatasetInstanceProperties props)
    throws DatasetManagementException {

    HttpResponse response = doPost("instances/" + datasetInstanceName,
                                   GSON.toJson(props),
                                   ImmutableMap.of("type-name", datasetType));
    if (HttpResponseStatus.CONFLICT.getCode() == response.responseCode) {
      throw new InstanceConflictException(String.format("Failed to add instance %s due to conflict, details: %s",
                                                        datasetInstanceName, getDetails(response)));
    }
    if (HttpResponseStatus.OK.getCode() != response.responseCode) {
      throw new DatasetManagementException(String.format("Failed to add instance %s, details: %s",
                                                         datasetInstanceName, getDetails(response)));
    }
  }

  public void deleteInstance(String datasetInstanceName) throws DatasetManagementException {
    HttpResponse response = doDelete("instances/" + datasetInstanceName);
    if (HttpResponseStatus.CONFLICT.getCode() == response.responseCode) {
      throw new InstanceConflictException(String.format("Failed to delete instance %s due to conflict, details: %s",
                                                        datasetInstanceName, getDetails(response)));
    }
    if (HttpResponseStatus.OK.getCode() != response.responseCode) {
      throw new DatasetManagementException(String.format("Failed to delete instance %s, details: %s",
                                                         datasetInstanceName, getDetails(response)));
    }
  }

  public void addModule(String moduleName, String className, Location jarLocation)
    throws DatasetManagementException {

    InputStream is;
    try {
      is = jarLocation.getInputStream();
    } catch (IOException e) {
      throw new DatasetManagementException(String.format("Failed to read jar of module %s at %s",
                                                         moduleName, jarLocation));
    }
    HttpResponse response;
    try {
      response = doRequest("modules/" + moduleName,
                       "POST",
                       ImmutableMap.of("class-name", className),
                       null, is);
    } finally {
      Closeables.closeQuietly(is);
    }

    if (HttpResponseStatus.CONFLICT.getCode() == response.responseCode) {
      throw new ModuleConflictException(String.format("Failed to add module %s due to conflict, details: %s",
                                                      moduleName, getDetails(response)));
    }
    if (HttpResponseStatus.OK.getCode() != response.responseCode) {
      throw new DatasetManagementException(String.format("Failed to add module %s, details: %s",
                                                      moduleName, getDetails(response)));
    }
  }


  public void deleteModule(String moduleName) throws DatasetManagementException {
    HttpResponse response = doDelete("modules/" + moduleName);

    if (HttpResponseStatus.CONFLICT.getCode() == response.responseCode) {
      throw new ModuleConflictException(String.format("Failed to delete module %s due to conflict, details: %s",
                                                      moduleName, getDetails(response)));
    }
    if (HttpResponseStatus.OK.getCode() != response.responseCode) {
      throw new DatasetManagementException(String.format("Failed to delete module %s, details: %s",
                                                         moduleName, getDetails(response)));
    }
  }

  private HttpResponse doGet(String resource) throws DatasetManagementException {
    return doRequest(resource, "GET", null, null, null);
  }

  private HttpResponse doPost(String resource, String body, Map<String, String> headers)
    throws DatasetManagementException {

    return doRequest(resource, "POST", headers, body, null);
  }

  private HttpResponse doDelete(String resource) throws DatasetManagementException {
    return doRequest(resource, "DELETE", null, null, null);
  }

  private DatasetManagerServiceClient.HttpResponse doRequest(String resource, String requestMethod,
                                                             @Nullable Map<String, String> headers,
                                                             @Nullable String body,
                                                             @Nullable InputStream bodySrc)
    throws DatasetManagementException {

    Preconditions.checkArgument(!(body != null && bodySrc != null), "only one of body and bodySrc can be used as body");

    String resolvedUrl = resolve(resource);
    try {
      URL url = new URL(resolvedUrl);
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

      } finally {
        conn.disconnect();
      }
    } catch (IOException e) {
      throw new DatasetManagementException(
        String.format("Error during talking to Dataset Service at %s while doing %s with headers %s and body %s",
                      resolvedUrl, requestMethod,
                      headers == null ? "null" : Joiner.on(",").withKeyValueSeparator("=").join(headers),
                      body == null ? bodySrc : body), e);
    }
  }

  private String getDetails(HttpResponse response) throws DatasetManagementException {
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
