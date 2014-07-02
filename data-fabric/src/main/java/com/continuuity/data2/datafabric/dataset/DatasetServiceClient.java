package com.continuuity.data2.datafabric.dataset;

import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.discovery.TimeLimitEndpointStrategy;
import com.continuuity.common.http.HttpRequests;
import com.continuuity.common.http.HttpResponse;
import com.continuuity.data2.datafabric.dataset.service.DatasetInstanceHandler;
import com.continuuity.data2.datafabric.dataset.service.DatasetInstanceMeta;
import com.continuuity.data2.datafabric.dataset.type.DatasetModuleMeta;
import com.continuuity.data2.datafabric.dataset.type.DatasetTypeMeta;
import com.continuuity.data2.dataset2.DatasetManagementException;
import com.continuuity.data2.dataset2.InstanceConflictException;
import com.continuuity.data2.dataset2.ModuleConflictException;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closeables;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Provides programmatic APIs to access {@link com.continuuity.data2.datafabric.dataset.service.DatasetService}.
 * Just a java wrapper for accessing service's REST API.
 */
class DatasetServiceClient {
  private static final Gson GSON = new Gson();

  private final Supplier<EndpointStrategy> endpointStrategySupplier;

  public DatasetServiceClient(final DiscoveryServiceClient discoveryClient) {
    this.endpointStrategySupplier = Suppliers.memoize(new Supplier<EndpointStrategy>() {
      @Override
      public EndpointStrategy get() {
        return new TimeLimitEndpointStrategy(
          new RandomEndpointStrategy(discoveryClient.discover(Constants.Service.DATASET_MANAGER)),
          1L, TimeUnit.SECONDS);
      }
    });
  }

  @Nullable
  public DatasetInstanceMeta getInstance(String instanceName) throws DatasetManagementException {
    HttpResponse response = doGet("datasets/" + instanceName);
    if (HttpResponseStatus.NOT_FOUND.getCode() == response.getResponseCode()) {
      return null;
    }
    if (HttpResponseStatus.OK.getCode() != response.getResponseCode()) {
      throw new DatasetManagementException(String.format("Cannot retrieve dataset instance %s info, details: %s",
                                                         instanceName, getDetails(response)));
    }

    return GSON.fromJson(new String(response.getResponseBody(), Charsets.UTF_8), DatasetInstanceMeta.class);
  }

  public Collection<DatasetSpecification> getAllInstances() throws DatasetManagementException {
    HttpResponse response = doGet("datasets");
    if (HttpResponseStatus.OK.getCode() != response.getResponseCode()) {
      throw new DatasetManagementException(String.format("Cannot retrieve all dataset instances, details: %s",
                                                         getDetails(response)));
    }

    return GSON.fromJson(new String(response.getResponseBody(), Charsets.UTF_8),
                         new TypeToken<List<DatasetSpecification>>() { }.getType());
  }

  public Collection<DatasetModuleMeta> getAllModules() throws DatasetManagementException {
    HttpResponse response = doGet("modules");
    if (HttpResponseStatus.OK.getCode() != response.getResponseCode()) {
      throw new DatasetManagementException(String.format("Cannot retrieve all dataset instances, details: %s",
                                                         getDetails(response)));
    }

    return GSON.fromJson(new String(response.getResponseBody(), Charsets.UTF_8),
                         new TypeToken<List<DatasetModuleMeta>>() { }.getType());
  }

  public DatasetTypeMeta getType(String typeName) throws DatasetManagementException {
    HttpResponse response = doGet("types/" + typeName);
    if (HttpResponseStatus.NOT_FOUND.getCode() == response.getResponseCode()) {
      return null;
    }
    if (HttpResponseStatus.OK.getCode() != response.getResponseCode()) {
      throw new DatasetManagementException(String.format("Cannot retrieve dataset type %s info, details: %s",
                                                         typeName, getDetails(response)));
    }
    return GSON.fromJson(new String(response.getResponseBody(), Charsets.UTF_8), DatasetTypeMeta.class);
  }

  public void addInstance(String datasetInstanceName, String datasetType, DatasetProperties props)
    throws DatasetManagementException {

    DatasetInstanceHandler.DatasetTypeAndProperties typeAndProps =
      new DatasetInstanceHandler.DatasetTypeAndProperties(datasetType, props.getProperties());
    HttpResponse response = doPut("datasets/" + datasetInstanceName, GSON.toJson(typeAndProps));
    if (HttpResponseStatus.CONFLICT.getCode() == response.getResponseCode()) {
      throw new InstanceConflictException(String.format("Failed to add instance %s due to conflict, details: %s",
                                                        datasetInstanceName, getDetails(response)));
    }
    if (HttpResponseStatus.OK.getCode() != response.getResponseCode()) {
      throw new DatasetManagementException(String.format("Failed to add instance %s, details: %s",
                                                         datasetInstanceName, getDetails(response)));
    }
  }

  public void deleteInstance(String datasetInstanceName) throws DatasetManagementException {
    HttpResponse response = doDelete("datasets/" + datasetInstanceName);
    if (HttpResponseStatus.CONFLICT.getCode() == response.getResponseCode()) {
      throw new InstanceConflictException(String.format("Failed to delete instance %s due to conflict, details: %s",
                                                        datasetInstanceName, getDetails(response)));
    }
    if (HttpResponseStatus.OK.getCode() != response.getResponseCode()) {
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
                       "PUT",
                       ImmutableMap.of("X-Continuuity-Class-Name", className),
                       null, is);
    } finally {
      Closeables.closeQuietly(is);
    }

    if (HttpResponseStatus.CONFLICT.getCode() == response.getResponseCode()) {
      throw new ModuleConflictException(String.format("Failed to add module %s due to conflict, details: %s",
                                                      moduleName, getDetails(response)));
    }
    if (HttpResponseStatus.OK.getCode() != response.getResponseCode()) {
      throw new DatasetManagementException(String.format("Failed to add module %s, details: %s",
                                                      moduleName, getDetails(response)));
    }
  }


  public void deleteModule(String moduleName) throws DatasetManagementException {
    HttpResponse response = doDelete("modules/" + moduleName);

    if (HttpResponseStatus.CONFLICT.getCode() == response.getResponseCode()) {
      throw new ModuleConflictException(String.format("Failed to delete module %s due to conflict, details: %s",
                                                      moduleName, getDetails(response)));
    }
    if (HttpResponseStatus.OK.getCode() != response.getResponseCode()) {
      throw new DatasetManagementException(String.format("Failed to delete module %s, details: %s",
                                                         moduleName, getDetails(response)));
    }
  }

  public void deleteModules() throws DatasetManagementException {
    HttpResponse response = doDelete("modules");

    if (HttpResponseStatus.OK.getCode() != response.getResponseCode()) {
      throw new DatasetManagementException(String.format("Failed to delete modules, details: %s",
                                                         getDetails(response)));
    }
  }

  public void deleteInstances() throws DatasetManagementException {
    HttpResponse response = doDelete("unrecoverable/datasets");

    if (HttpResponseStatus.OK.getCode() != response.getResponseCode()) {
      throw new DatasetManagementException(String.format("Failed to delete instances, details: %s",
                                                         getDetails(response)));
    }
  }

  private HttpResponse doGet(String resource) throws DatasetManagementException {
    return doRequest(resource, "GET", null, null, null);
  }

  private HttpResponse doPut(String resource, String body)
    throws DatasetManagementException {

    return doRequest(resource, "PUT", null, body, null);
  }

  private HttpResponse doDelete(String resource) throws DatasetManagementException {
    return doRequest(resource, "DELETE", null, null, null);
  }

  private HttpResponse doRequest(String resource, String requestMethod,
                                 @Nullable Map<String, String> headers,
                                 @Nullable String body,
                                 @Nullable InputStream bodySrc)
    throws DatasetManagementException {

    Preconditions.checkArgument(!(body != null && bodySrc != null), "only one of body and bodySrc can be used as body");

    String resolvedUrl = resolve(resource);
    try {
      URL url = new URL(resolvedUrl);
      return HttpRequests.doRequest(requestMethod, url, headers, body, bodySrc);
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
                         response.getResponseCode(), response.getResponseMessage(),
                         response.getResponseBody() == null ?
                           "null" : new String(response.getResponseBody(), Charsets.UTF_8));

  }

  private String resolve(String resource) {
    InetSocketAddress addr = this.endpointStrategySupplier.get().pick().getSocketAddress();
    return String.format("http://%s:%s%s/data/%s", addr.getHostName(), addr.getPort(),
                         Constants.Gateway.GATEWAY_VERSION, resource);
  }
}
