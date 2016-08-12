/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.InstanceConflictException;
import co.cask.cdap.api.dataset.InstanceNotFoundException;
import co.cask.cdap.common.ServiceUnavailableException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.EndpointStrategy;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.common.http.DefaultHttpRequestConfig;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.kerberos.SecurityUtil;
import co.cask.cdap.data2.dataset2.ModuleConflictException;
import co.cask.cdap.proto.DatasetInstanceConfiguration;
import co.cask.cdap.proto.DatasetMeta;
import co.cask.cdap.proto.DatasetModuleMeta;
import co.cask.cdap.proto.DatasetSpecificationSummary;
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequestConfig;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.io.InputSupplier;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Provides programmatic APIs to access {@link co.cask.cdap.data2.datafabric.dataset.service.DatasetService}.
 * Just a java wrapper for accessing service's REST API.
 */
class DatasetServiceClient {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetServiceClient.class);
  private static final Gson GSON = new Gson();
  private static final Type SUMMARY_LIST_TYPE = new TypeToken<List<DatasetSpecificationSummary>>() { }.getType();
  private static final Type MODULE_META_LIST_TYPE = new TypeToken<List<DatasetModuleMeta>>() { }.getType();
  private static final Type DATASET_NAME_TYPE = new TypeToken<Set<String>>() { }.getType();

  private final Supplier<EndpointStrategy> endpointStrategySupplier;
  private final NamespaceId namespaceId;
  private final HttpRequestConfig httpRequestConfig;
  private final boolean securityEnabled;
  private final boolean kerberosEnabled;
  private final AuthenticationContext authenticationContext;
  private final String masterPrincipal;
  private final boolean isMasterUser;

  DatasetServiceClient(final DiscoveryServiceClient discoveryClient, NamespaceId namespaceId,
                       CConfiguration cConf, AuthenticationContext authenticationContext) {
    this.endpointStrategySupplier = Suppliers.memoize(new Supplier<EndpointStrategy>() {
      @Override
      public EndpointStrategy get() {
        return new RandomEndpointStrategy(discoveryClient.discover(Constants.Service.DATASET_MANAGER));
      }
    });
    this.namespaceId = namespaceId;
    this.httpRequestConfig = new DefaultHttpRequestConfig();
    this.securityEnabled = cConf.getBoolean(Constants.Security.ENABLED);
    this.kerberosEnabled = SecurityUtil.isKerberosEnabled(cConf);
    this.authenticationContext = authenticationContext;
    this.masterPrincipal = cConf.get(Constants.Security.CFG_CDAP_MASTER_KRB_PRINCIPAL);

    try {
      if (securityEnabled && kerberosEnabled) {
        // we compare short name, because in some YARN containers launched by CDAP, the current username isn't the full
        // configured principal
        String currUserShortName = UserGroupInformation.getCurrentUser().getShortUserName();
        this.isMasterUser = currUserShortName.equals(new KerberosName(masterPrincipal).getShortName());
      } else {
        this.isMasterUser = false;
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @Nullable
  public DatasetMeta getInstance(String instanceName, @Nullable Iterable<? extends Id> owners)
    throws DatasetManagementException {

    String query = "";
    if (owners != null) {
      Set<String> ownerParams = Sets.newHashSet();
      for (Id owner : owners) {
        ownerParams.add("owner=" + owner.toString());
      }
      query = ownerParams.isEmpty() ? "" : "?" + Joiner.on("&").join(ownerParams);
    }

    HttpResponse response = doGet("datasets/" + instanceName + query);
    if (HttpResponseStatus.NOT_FOUND.getCode() == response.getResponseCode()) {
      return null;
    }
    if (HttpResponseStatus.OK.getCode() != response.getResponseCode()) {
      throw new DatasetManagementException(String.format("Cannot retrieve dataset instance %s info, details: %s",
                                                         instanceName, response));
    }

    return GSON.fromJson(response.getResponseBodyAsString(), DatasetMeta.class);
  }

  @Nullable
  public DatasetMeta getInstance(String instanceName) throws DatasetManagementException {
    return getInstance(instanceName, null);
  }

  public Collection<DatasetSpecificationSummary> getAllInstances() throws DatasetManagementException {
    HttpResponse response = doGet("datasets");
    if (HttpResponseStatus.OK.getCode() != response.getResponseCode()) {
      throw new DatasetManagementException(String.format("Cannot retrieve all dataset instances, details: %s",
                                                         response));
    }

    return GSON.fromJson(response.getResponseBodyAsString(), SUMMARY_LIST_TYPE);
  }

  public Collection<DatasetModuleMeta> getAllModules() throws DatasetManagementException {
    HttpResponse response = doGet("modules");
    if (HttpResponseStatus.OK.getCode() != response.getResponseCode()) {
      throw new DatasetManagementException(String.format("Cannot retrieve all dataset instances, details: %s",
                                                         response));
    }

    return GSON.fromJson(response.getResponseBodyAsString(), MODULE_META_LIST_TYPE);
  }

  @Nullable
  public DatasetTypeMeta getType(String typeName) throws DatasetManagementException {
    HttpResponse response = doGet("types/" + typeName);
    if (HttpResponseStatus.NOT_FOUND.getCode() == response.getResponseCode()) {
      return null;
    }
    if (HttpResponseStatus.OK.getCode() != response.getResponseCode()) {
      throw new DatasetManagementException(String.format("Cannot retrieve dataset type %s info, details: %s",
                                                         typeName, response));
    }
    return GSON.fromJson(response.getResponseBodyAsString(), DatasetTypeMeta.class);
  }

  public void addInstance(String datasetInstanceName, String datasetType, DatasetProperties props)
    throws DatasetManagementException {
    DatasetInstanceConfiguration creationProperties =
      new DatasetInstanceConfiguration(datasetType, props.getProperties(), props.getDescription());

    HttpResponse response = doPut("datasets/" + datasetInstanceName, GSON.toJson(creationProperties));

    if (HttpResponseStatus.CONFLICT.getCode() == response.getResponseCode()) {
      throw new InstanceConflictException(String.format("Failed to add instance %s due to conflict, details: %s",
                                                         datasetInstanceName, response));
    }
    if (HttpResponseStatus.OK.getCode() != response.getResponseCode()) {
      throw new DatasetManagementException(String.format("Failed to add instance %s, details: %s",
                                                          datasetInstanceName, response));
    }
  }

  public void updateInstance(String datasetInstanceName, DatasetProperties props) throws DatasetManagementException {

    HttpResponse response = doPut("datasets/" + datasetInstanceName + "/properties",
                                  GSON.toJson(props.getProperties()));

    if (HttpResponseStatus.NOT_FOUND.getCode() == response.getResponseCode()) {
      throw new InstanceNotFoundException(datasetInstanceName);
    }
    if (HttpResponseStatus.CONFLICT.getCode() == response.getResponseCode()) {
      throw new InstanceConflictException(String.format("Failed to update instance %s due to conflict, details: %s",
                                                        datasetInstanceName, response));
    }
    if (HttpResponseStatus.OK.getCode() != response.getResponseCode()) {
      throw new DatasetManagementException(String.format("Failed to update instance %s, details: %s",
                                                         datasetInstanceName, response));
    }
  }

  public void truncateInstance(String datasetInstanceName) throws DatasetManagementException {
    HttpResponse response = doPost("datasets/" + datasetInstanceName + "/admin/truncate");
    if (HttpResponseStatus.NOT_FOUND.getCode() == response.getResponseCode()) {
      throw new InstanceNotFoundException(datasetInstanceName);
    }
    if (HttpResponseStatus.OK.getCode() != response.getResponseCode()) {
      throw new DatasetManagementException(String.format("Failed to truncate instance %s, details: %s",
                                                         datasetInstanceName, response));
    }
  }

  public void deleteInstance(String datasetInstanceName) throws DatasetManagementException {
    HttpResponse response = doDelete("datasets/" + datasetInstanceName);
    if (HttpResponseStatus.NOT_FOUND.getCode() == response.getResponseCode()) {
      throw new InstanceNotFoundException(datasetInstanceName);
    }
    if (HttpResponseStatus.CONFLICT.getCode() == response.getResponseCode()) {
      throw new InstanceConflictException(String.format("Failed to delete instance %s due to conflict, details: %s",
                                                        datasetInstanceName, response));
    }
    if (HttpResponseStatus.OK.getCode() != response.getResponseCode()) {
      throw new DatasetManagementException(String.format("Failed to delete instance %s, details: %s",
                                                         datasetInstanceName, response));
    }
  }

  /**
   * Deletes all dataset instances inside the namespace of this client is operating in.
   */
  public Set<String> deleteInstances() throws DatasetManagementException {
    HttpResponse response = doDelete("datasets");
    if (HttpResponseStatus.OK.getCode() != response.getResponseCode()) {
      throw new DatasetManagementException(String.format("Failed to delete instances, details: %s", response));
    }

    return GSON.fromJson(response.getResponseBodyAsString(), DATASET_NAME_TYPE);
  }

  public void addModule(String moduleName, String className, Location jarLocation) throws DatasetManagementException {

    HttpResponse response = doRequest(HttpMethod.PUT, "modules/" + moduleName,
                                      ImmutableMultimap.of("X-Class-Name", className),
                                      Locations.newInputSupplier(jarLocation));

    if (HttpResponseStatus.CONFLICT.getCode() == response.getResponseCode()) {
      throw new ModuleConflictException(String.format("Failed to add module %s due to conflict, details: %s",
                                                      moduleName, response));
    }
    if (HttpResponseStatus.OK.getCode() != response.getResponseCode()) {
      throw new DatasetManagementException(String.format("Failed to add module %s, details: %s", moduleName, response));
    }
  }


  public void deleteModule(String moduleName) throws DatasetManagementException {
    HttpResponse response = doDelete("modules/" + moduleName);

    if (HttpResponseStatus.CONFLICT.getCode() == response.getResponseCode()) {
      throw new ModuleConflictException(String.format("Failed to delete module %s due to conflict, details: %s",
                                                      moduleName, response));
    }
    if (HttpResponseStatus.OK.getCode() != response.getResponseCode()) {
      throw new DatasetManagementException(String.format("Failed to delete module %s, details: %s",
                                                         moduleName, response));
    }
  }

  public void deleteModules() throws DatasetManagementException {
    HttpResponse response = doDelete("modules");

    if (HttpResponseStatus.OK.getCode() != response.getResponseCode()) {
      throw new DatasetManagementException(String.format("Failed to delete modules, details: %s", response));
    }
  }

  private HttpResponse doGet(String resource) throws DatasetManagementException {
    return doRequest(HttpMethod.GET, resource);
  }

  private HttpResponse doPut(String resource, String body) throws DatasetManagementException {
    return doRequest(HttpMethod.PUT, resource, null, body);
  }

  private HttpResponse doPost(String resource) throws DatasetManagementException {
    return doRequest(HttpMethod.POST, resource);
  }

  private HttpResponse doDelete(String resource) throws DatasetManagementException {
    return doRequest(HttpMethod.DELETE, resource);
  }

  private HttpResponse doRequest(HttpMethod method, String resource, @Nullable Multimap<String, String> headers,
                                 String body) throws DatasetManagementException {
    String url = resolve(resource);
    try {
      return HttpRequests.execute(addUserIdHeader(
        HttpRequest.builder(method, new URL(url))
          .addHeaders(headers)
          .withBody(body)
      ).build(), httpRequestConfig);
    } catch (IOException e) {
      throw new DatasetManagementException(
        String.format("Error during talking to Dataset Service at %s while doing %s with headers %s and body %s",
                      url, method,
                      headers == null ? "null" : Joiner.on(",").withKeyValueSeparator("=").join(headers.entries()),
                      body == null ? "null" : body), e);
    }
  }

  private HttpResponse doRequest(HttpMethod method, String resource,
                                 @Nullable Multimap<String, String> headers,
                                 @Nullable InputSupplier<? extends InputStream> body)
    throws DatasetManagementException {

    String url = resolve(resource);
    try {
      return HttpRequests.execute(addUserIdHeader(
        HttpRequest.builder(method, new URL(url))
          .addHeaders(headers)
          .withBody(body)
      ).build(), httpRequestConfig);
    } catch (IOException e) {
      throw new DatasetManagementException(
        String.format("Error during talking to Dataset Service at %s while doing %s with headers %s and body %s",
                      url, method,
                      headers == null ? "null" : Joiner.on(",").withKeyValueSeparator("=").join(headers.entries()),
                      body == null ? "null" : body), e);
    }
  }

  private HttpRequest.Builder addUserIdHeader(HttpRequest.Builder builder) throws IOException {
    if (!securityEnabled) {
      return builder;
    }
    String userId;
    if (NamespaceId.SYSTEM.equals(namespaceId) &&
      (!kerberosEnabled || isMasterUser)) {
      // For getting a system dataset like MDS, use the system principal, if the current user is the same as the
      // CDAP kerberos principal. If a user tries to access a system dataset from an app, either:
      // 1. The request will go through if the user is impersonating as the cdap principal - which means that
      // impersonation has specifically been configured in this namespace to use the cdap principal; or
      // 2. The request will fail, if kerberos is enabled and the user is impersonating as a non-cdap user; or
      // 3. The request will go through, if kerberos is disabled
      LOG.trace("Acessing dataset in system namespace using the system principal because the current user's " +
                  "kerberos principal {} is the same as the CDAP master's kerberos principal {}.",
                masterPrincipal, UserGroupInformation.getCurrentUser().getUserName());
      userId = Principal.SYSTEM.getName();
    } else {
      // If the request originated from the router and was forwarded to any service other than dataset service, before
      // going to dataset service via dataset service client, the userId could be set in the SecurityRequestContext.
      // e.g. deploying an app that contains a dataset.
      // For user datasets, if a dataset call originates from a program runtime, then find the userId from
      // UserGroupInformation#getCurrentUser()
      userId = authenticationContext.getPrincipal().getName();
    }
    return builder.addHeader(Constants.Security.Headers.USER_ID, userId);
  }

  private HttpResponse doRequest(HttpMethod method, String url) throws DatasetManagementException {
    return doRequest(method, url, null, (InputSupplier<? extends InputStream>) null);
  }

  private String resolve(String resource) throws DatasetManagementException {
    Discoverable discoverable = endpointStrategySupplier.get().pick(3, TimeUnit.SECONDS);
    if (discoverable == null) {
      throw new ServiceUnavailableException("DatasetService");
    }
    InetSocketAddress addr = discoverable.getSocketAddress();
    return String.format("http://%s:%s%s/namespaces/%s/data/%s", addr.getHostName(), addr.getPort(),
                         Constants.Gateway.API_VERSION_3, namespaceId.getNamespace(), resource);
  }
}
