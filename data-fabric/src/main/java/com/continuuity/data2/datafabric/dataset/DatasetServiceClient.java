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

package com.continuuity.data2.datafabric.dataset;

import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.discovery.TimeLimitEndpointStrategy;
import com.continuuity.common.http.HttpMethod;
import com.continuuity.common.http.HttpRequest;
import com.continuuity.common.http.HttpRequests;
import com.continuuity.common.http.HttpResponse;
import com.continuuity.data2.dataset2.DatasetManagementException;
import com.continuuity.data2.dataset2.InstanceConflictException;
import com.continuuity.data2.dataset2.ModuleConflictException;
import com.continuuity.proto.DatasetInstanceConfiguration;
import com.continuuity.proto.DatasetMeta;
import com.continuuity.proto.DatasetModuleMeta;
import com.continuuity.proto.DatasetTypeMeta;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closeables;
import com.google.common.io.InputSupplier;
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
      final InputStream inputStream = is;
      response = doRequest(HttpMethod.PUT, "modules/" + moduleName,
                       ImmutableMap.of("X-Continuuity-Class-Name", className),
                       new InputSupplier<InputStream>() {
                         @Override
                         public InputStream getInput() throws IOException {
                           return inputStream;
                         }
                       });
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
      return HttpRequests.execute(HttpRequest.builder(method, new URL(url)).addHeaders(headers).withBody(body).build());
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
      return HttpRequests.execute(HttpRequest.builder(method, new URL(url)).addHeaders(headers).withBody(body).build());
    } catch (IOException e) {
      throw new DatasetManagementException(
        String.format("Error during talking to Dataset Service at %s while doing %s with headers %s and body %s",
                      url, method, headers == null ? "null" : Joiner.on(",").withKeyValueSeparator("=").join(headers),
                      body == null ? "null" : body), e);
    }
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

  private String resolve(String resource) {
    InetSocketAddress addr = this.endpointStrategySupplier.get().pick().getSocketAddress();
    return String.format("http://%s:%s%s/data/%s", addr.getHostName(), addr.getPort(),
                         Constants.Gateway.GATEWAY_VERSION, resource);
  }
}
