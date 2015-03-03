/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.data2.datafabric.dataset;

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.EndpointStrategy;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.common.http.SecurityRequestContext;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.dataset2.InstanceConflictException;
import co.cask.cdap.data2.dataset2.ModuleConflictException;
import co.cask.cdap.proto.DatasetInstanceConfiguration;
import co.cask.cdap.proto.DatasetMeta;
import co.cask.cdap.proto.DatasetSpecificationSummary;
import co.cask.cdap.proto.DatasetModuleMeta;
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.cdap.proto.Id;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.InputSupplier;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Provides programmatic APIs to access {@link co.cask.cdap.data2.datafabric.dataset.service.DatasetService}.
 * Just a java wrapper for accessing service's REST API.
 */
class DatasetServiceClient {
  private static final Gson GSON = new Gson();
  private static final Type META_SUMMARY_LIST_TYPE = new TypeToken<List<DatasetSpecificationSummary>>() { }.getType();
  private static final Type MODULE_META_LIST_TYPE = new TypeToken<List<DatasetModuleMeta>>() { }.getType();

  private final Supplier<EndpointStrategy> endpointStrategySupplier;
  private final Id.Namespace namespaceId;

  public DatasetServiceClient(final DiscoveryServiceClient discoveryClient, Id.Namespace namespaceId) {
    this.endpointStrategySupplier = Suppliers.memoize(new Supplier<EndpointStrategy>() {
      @Override
      public EndpointStrategy get() {
        return new RandomEndpointStrategy(discoveryClient.discover(Constants.Service.DATASET_MANAGER));
      }
    });
    this.namespaceId = namespaceId;
  }

  @Nullable
  public DatasetMeta getInstance(String instanceName) throws DatasetManagementException {
    HttpResponse response = doGet("datasets/" + instanceName);
    if (HttpResponseStatus.NOT_FOUND.getCode() == response.getResponseCode()) {
      return null;
    }
    if (HttpResponseStatus.OK.getCode() != response.getResponseCode()) {
      throw new DatasetManagementException(String.format("Cannot retrieve dataset instance %s info, details: %s",
                                                         instanceName, getDetails(response)));
    }

    return GSON.fromJson(new String(response.getResponseBody(), Charsets.UTF_8), DatasetMeta.class);
  }

  public Collection<DatasetSpecificationSummary> getAllInstances() throws DatasetManagementException {
    HttpResponse response = doGet("datasets");
    if (HttpResponseStatus.OK.getCode() != response.getResponseCode()) {
      throw new DatasetManagementException(String.format("Cannot retrieve all dataset instances, details: %s",
                                                         getDetails(response)));
    }

    return GSON.fromJson(new String(response.getResponseBody(), Charsets.UTF_8), META_SUMMARY_LIST_TYPE);
  }

  public Collection<DatasetModuleMeta> getAllModules() throws DatasetManagementException {
    HttpResponse response = doGet("modules");
    if (HttpResponseStatus.OK.getCode() != response.getResponseCode()) {
      throw new DatasetManagementException(String.format("Cannot retrieve all dataset instances, details: %s",
                                                         getDetails(response)));
    }

    return GSON.fromJson(new String(response.getResponseBody(), Charsets.UTF_8), MODULE_META_LIST_TYPE);
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
    DatasetInstanceConfiguration creationProperties = new DatasetInstanceConfiguration(datasetType,
                                                                                       props.getProperties());

    HttpResponse response = doPut("datasets/" + datasetInstanceName, GSON.toJson(creationProperties));

    if (HttpResponseStatus.CONFLICT.getCode() == response.getResponseCode()) {
      throw new InstanceConflictException(String.format("Failed to add instance %s due to conflict, details: %s",
                                                         datasetInstanceName, getDetails(response)));
    }
    if (HttpResponseStatus.OK.getCode() != response.getResponseCode()) {
      throw new DatasetManagementException(String.format("Failed to add instance %s, details: %s",
                                                          datasetInstanceName, getDetails(response)));
    }
  }

  public void updateInstance(String datasetInstanceName, DatasetProperties props)
    throws DatasetManagementException {
    DatasetMeta meta = getInstance(datasetInstanceName);
    DatasetInstanceConfiguration creationProperties =
      new DatasetInstanceConfiguration(meta.getSpec().getType(), props.getProperties());

    HttpResponse response = doPut("datasets/" + datasetInstanceName + "/properties", GSON.toJson(creationProperties));

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

    HttpResponse response = doRequest(HttpMethod.PUT, "modules/" + moduleName,
                           ImmutableMap.of("X-Class-Name", className),
                           Locations.newInputSupplier(jarLocation));

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

  public void createNamespace() throws DatasetManagementException {
    HttpResponse response = doPut("admin/create", GSON.toJson(namespaceId));
    if (HttpResponseStatus.OK.getCode() != response.getResponseCode()) {
      throw new DatasetManagementException(String.format("Failed to create namespace, details: %s",
                                                         getDetails(response)));
    }
  }

  public void deleteNamespace() throws DatasetManagementException {
    HttpResponse response = doDelete("admin/delete");
    if (HttpResponseStatus.OK.getCode() != response.getResponseCode()) {
      throw new DatasetManagementException(String.format("Failed to delete namespace, details: %s",
                                                         getDetails(response)));
    }
  }

  private HttpResponse doGet(String resource) throws DatasetManagementException {
    return doRequest(HttpMethod.GET, resource);
  }

  private HttpResponse doPut(String resource, String body)
    throws DatasetManagementException {

    return doRequest(HttpMethod.PUT, resource, null, body);
  }

  private HttpResponse doDelete(String resource) throws DatasetManagementException {
    return doRequest(HttpMethod.DELETE, resource);
  }

  private HttpResponse doRequest(HttpMethod method, String resource,
                                 @Nullable Map<String, String> headers,
                                 @Nullable String body) throws DatasetManagementException {

    String url = resolve(resource);
    try {
      return HttpRequests.execute(processBuilder(
        HttpRequest.builder(method, new URL(url))
          .addHeaders(headers)
          .withBody(body)
      ).build());
    } catch (IOException e) {
      throw new DatasetManagementException(
        String.format("Error during talking to Dataset Service at %s while doing %s with headers %s and body %s",
                      url, method, headers == null ? "null" : Joiner.on(",").withKeyValueSeparator("=").join(headers),
                      body == null ? "null" : body), e);
    }
  }

  private HttpResponse doRequest(HttpMethod method, String resource,
                                 @Nullable Map<String, String> headers,
                                 @Nullable InputSupplier<? extends InputStream> body)
    throws DatasetManagementException {

    String url = resolve(resource);
    try {
      return HttpRequests.execute(processBuilder(
        HttpRequest.builder(method, new URL(url))
          .addHeaders(headers)
          .withBody(body)
      ).build());
    } catch (IOException e) {
      throw new DatasetManagementException(
        String.format("Error during talking to Dataset Service at %s while doing %s with headers %s and body %s",
                      url, method, headers == null ? "null" : Joiner.on(",").withKeyValueSeparator("=").join(headers),
                      body == null ? "null" : body), e);
    }
  }

  private HttpRequest.Builder processBuilder(HttpRequest.Builder builder) {
    if (SecurityRequestContext.getUserId().isPresent()) {
      builder.addHeader(Constants.Security.Headers.USER_ID, SecurityRequestContext.getUserId().get());
    }
    return builder;
  }

  private HttpResponse doRequest(HttpMethod method, String url) throws DatasetManagementException {
    return doRequest(method, url, null, (InputSupplier<? extends InputStream>) null);
  }

  private String getDetails(HttpResponse response) throws DatasetManagementException {
    return String.format("Response code: %s, message:'%s', body: '%s'",
                         response.getResponseCode(), response.getResponseMessage(),
                         response.getResponseBody() == null ?
                           "null" : new String(response.getResponseBody(), Charsets.UTF_8));

  }

  private String resolve(String resource) throws DatasetManagementException {
    Discoverable discoverable = endpointStrategySupplier.get().pick(1, TimeUnit.SECONDS);
    if (discoverable == null) {
      throw new DatasetManagementException("Cannot discover dataset service");
    }
    InetSocketAddress addr = discoverable.getSocketAddress();
    return String.format("http://%s:%s%s/namespaces/%s/data/%s", addr.getHostName(), addr.getPort(),
                         Constants.Gateway.API_VERSION_3, namespaceId.getId(), resource);
  }
}
