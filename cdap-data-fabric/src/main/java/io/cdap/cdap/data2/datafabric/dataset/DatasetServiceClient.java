/*
 * Copyright © 2014-2018 Cask Data, Inc.
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

package io.cdap.cdap.data2.datafabric.dataset;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.InstanceConflictException;
import io.cdap.cdap.api.dataset.InstanceNotFoundException;
import io.cdap.cdap.common.ServiceUnavailableException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.data2.dataset2.ModuleConflictException;
import io.cdap.cdap.proto.DatasetInstanceConfiguration;
import io.cdap.cdap.proto.DatasetMeta;
import io.cdap.cdap.proto.DatasetSpecificationSummary;
import io.cdap.cdap.proto.DatasetTypeMeta;
import io.cdap.cdap.proto.id.KerberosPrincipalId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.authorization.AuthorizationUtil;
import io.cdap.cdap.security.impersonation.SecurityUtil;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.ContentProvider;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Type;
import java.net.SocketTimeoutException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Provides programmatic APIs to access {@link io.cdap.cdap.data2.datafabric.dataset.service.DatasetService}.
 * Just a java wrapper for accessing service's REST API.
 */
public class DatasetServiceClient {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetServiceClient.class);
  private static final Gson GSON = new Gson();
  private static final Type SUMMARY_LIST_TYPE = new TypeToken<List<DatasetSpecificationSummary>>() { }.getType();

  private final RemoteClient remoteClient;
  private final NamespaceId namespaceId;
  private final boolean securityEnabled;
  private final boolean kerberosEnabled;
  private final boolean authorizationEnabled;
  private final AuthenticationContext authenticationContext;
  private final String masterShortUserName;

  DatasetServiceClient(final DiscoveryServiceClient discoveryClient, NamespaceId namespaceId,
                       CConfiguration cConf, AuthenticationContext authenticationContext) {
    this.remoteClient = new RemoteClient(
      discoveryClient, Constants.Service.DATASET_MANAGER, new DefaultHttpRequestConfig(false),
      String.format("%s/namespaces/%s/data", Constants.Gateway.API_VERSION_3, namespaceId.getNamespace()));
    this.namespaceId = namespaceId;
    this.securityEnabled = cConf.getBoolean(Constants.Security.ENABLED);
    this.kerberosEnabled = SecurityUtil.isKerberosEnabled(cConf);
    this.authorizationEnabled = cConf.getBoolean(Constants.Security.Authorization.ENABLED);
    this.authenticationContext = authenticationContext;
    this.masterShortUserName = AuthorizationUtil.getEffectiveMasterUser(cConf);
  }

  @Nullable
  public DatasetMeta getInstance(String instanceName)
    throws DatasetManagementException, UnauthorizedException {

    HttpResponse response = doGet("datasets/" + instanceName);
    if (HttpResponseStatus.NOT_FOUND.code() == response.getResponseCode()) {
      return null;
    }
    if (HttpResponseStatus.FORBIDDEN.code() == response.getResponseCode()) {
      throw new DatasetManagementException(String.format("Failed to get dataset instance %s, details: %s",
                                                         instanceName, response),
                                           new UnauthorizedException(response.getResponseBodyAsString()));
    }
    if (HttpResponseStatus.OK.code() != response.getResponseCode()) {
      throw new DatasetManagementException(String.format("Cannot retrieve dataset instance %s info, details: %s",
                                                         instanceName, response));
    }

    return GSON.fromJson(response.getResponseBodyAsString(), DatasetMeta.class);
  }

  Collection<DatasetSpecificationSummary> getAllInstances() throws DatasetManagementException, UnauthorizedException {
    HttpResponse response = doGet("datasets");
    if (HttpResponseStatus.OK.code() != response.getResponseCode()) {
      throw new DatasetManagementException(String.format("Cannot retrieve all dataset instances, details: %s",
                                                         response));
    }

    return GSON.fromJson(response.getResponseBodyAsString(), SUMMARY_LIST_TYPE);
  }

  /**
   * Get the dataset instances which have the specified dataset properties.
   */
  Collection<DatasetSpecificationSummary> getInstances(Map<String, String> properties)
    throws DatasetManagementException, UnauthorizedException {
    HttpResponse response = doPost("datasets", GSON.toJson(properties));
    if (HttpResponseStatus.OK.code() != response.getResponseCode()) {
      throw new DatasetManagementException(String.format("Cannot retrieve all dataset instances, details: %s",
                                                         response));
    }

    return GSON.fromJson(response.getResponseBodyAsString(), SUMMARY_LIST_TYPE);
  }

  @Nullable
  public DatasetTypeMeta getType(String typeName) throws DatasetManagementException, UnauthorizedException {
    HttpResponse response = doGet("types/" + typeName);
    if (HttpResponseStatus.NOT_FOUND.code() == response.getResponseCode()) {
      return null;
    }
    if (HttpResponseStatus.OK.code() != response.getResponseCode()) {
      throw new DatasetManagementException(String.format("Cannot retrieve dataset type %s info, details: %s",
                                                         typeName, response));
    }
    return GSON.fromJson(response.getResponseBodyAsString(), DatasetTypeMeta.class);
  }

  public void addInstance(String datasetInstanceName, String datasetType,
                          DatasetProperties props) throws DatasetManagementException, UnauthorizedException {
    addInstance(datasetInstanceName, datasetType, props, null);
  }

  public void addInstance(String datasetInstanceName, String datasetType, DatasetProperties props,
                          @Nullable KerberosPrincipalId owner)
    throws DatasetManagementException, UnauthorizedException {
    String ownerPrincipal = owner == null ? null : owner.getPrincipal();
    DatasetInstanceConfiguration creationProperties =
      new DatasetInstanceConfiguration(datasetType, props.getProperties(), props.getDescription(), ownerPrincipal);

    HttpResponse response = doPut("datasets/" + datasetInstanceName, GSON.toJson(creationProperties));

    if (HttpResponseStatus.CONFLICT.code() == response.getResponseCode()) {
      throw new InstanceConflictException(String.format("Failed to add instance %s due to conflict, details: %s",
                                                        datasetInstanceName, response));
    }
    if (HttpResponseStatus.FORBIDDEN.code() == response.getResponseCode()) {
      throw new DatasetManagementException(String.format("Failed to add instance %s, details: %s",
                                                         datasetInstanceName, response),
                                           new UnauthorizedException(response.getResponseBodyAsString()));
    }
    if (HttpResponseStatus.OK.code() != response.getResponseCode()) {
      throw new DatasetManagementException(String.format("Failed to add instance %s, details: %s",
                                                         datasetInstanceName, response));
    }
  }

  public void updateInstance(String datasetInstanceName, DatasetProperties props)
    throws DatasetManagementException, UnauthorizedException {

    HttpResponse response = doPut("datasets/" + datasetInstanceName + "/properties",
                                  GSON.toJson(props.getProperties()));

    if (HttpResponseStatus.NOT_FOUND.code() == response.getResponseCode()) {
      throw new InstanceNotFoundException(datasetInstanceName);
    }
    if (HttpResponseStatus.CONFLICT.code() == response.getResponseCode()) {
      throw new InstanceConflictException(String.format("Failed to update instance %s due to conflict, details: %s",
                                                        datasetInstanceName, response));
    }
    if (HttpResponseStatus.OK.code() != response.getResponseCode()) {
      throw new DatasetManagementException(String.format("Failed to update instance %s, details: %s",
                                                         datasetInstanceName, response));
    }
  }

  void truncateInstance(String datasetInstanceName) throws DatasetManagementException, UnauthorizedException {
    HttpResponse response = doPost("datasets/" + datasetInstanceName + "/admin/truncate");
    if (HttpResponseStatus.NOT_FOUND.code() == response.getResponseCode()) {
      throw new InstanceNotFoundException(datasetInstanceName);
    }
    if (HttpResponseStatus.OK.code() != response.getResponseCode()) {
      throw new DatasetManagementException(String.format("Failed to truncate instance %s, details: %s",
                                                         datasetInstanceName, response));
    }
  }

  public void deleteInstance(String datasetInstanceName) throws DatasetManagementException, UnauthorizedException {
    HttpResponse response = doDelete("datasets/" + datasetInstanceName);
    if (HttpResponseStatus.NOT_FOUND.code() == response.getResponseCode()) {
      throw new InstanceNotFoundException(datasetInstanceName);
    }
    if (HttpResponseStatus.CONFLICT.code() == response.getResponseCode()) {
      throw new InstanceConflictException(String.format("Failed to delete instance %s due to conflict, details: %s",
                                                        datasetInstanceName, response));
    }
    if (HttpResponseStatus.OK.code() != response.getResponseCode()) {
      throw new DatasetManagementException(String.format("Failed to delete instance %s, details: %s",
                                                         datasetInstanceName, response));
    }
  }

  /**
   * Deletes all dataset instances inside the namespace of this client is operating in.
   */
  void deleteInstances() throws DatasetManagementException, UnauthorizedException {
    HttpResponse response = doDelete("datasets");
    if (HttpResponseStatus.OK.code() != response.getResponseCode()) {
      throw new DatasetManagementException(String.format("Failed to delete instances, details: %s", response));
    }
  }

  public void addModule(String moduleName, String className, Location jarLocation)
    throws DatasetManagementException, UnauthorizedException {

    HttpRequest.Builder requestBuilder = remoteClient.requestBuilder(HttpMethod.PUT, "modules/" + moduleName)
      .addHeader("X-Class-Name", className)
      .withBody((ContentProvider<? extends InputStream>) jarLocation::getInputStream);
    HttpResponse response = doRequest(requestBuilder);

    if (HttpResponseStatus.CONFLICT.code() == response.getResponseCode()) {
      throw new ModuleConflictException(String.format("Failed to add module %s due to conflict, details: %s",
                                                      moduleName, response));
    }
    if (HttpResponseStatus.OK.code() != response.getResponseCode()) {
      throw new DatasetManagementException(String.format("Failed to add module %s, details: %s", moduleName, response));
    }
  }


  public void deleteModule(String moduleName) throws DatasetManagementException, UnauthorizedException {
    HttpResponse response = doDelete("modules/" + moduleName);

    if (HttpResponseStatus.CONFLICT.code() == response.getResponseCode()) {
      throw new ModuleConflictException(String.format("Failed to delete module %s due to conflict, details: %s",
                                                      moduleName, response));
    }
    if (HttpResponseStatus.OK.code() != response.getResponseCode()) {
      throw new DatasetManagementException(String.format("Failed to delete module %s, details: %s",
                                                         moduleName, response));
    }
  }

  void deleteModules() throws DatasetManagementException, UnauthorizedException {
    HttpResponse response = doDelete("modules");

    if (HttpResponseStatus.OK.code() != response.getResponseCode()) {
      throw new DatasetManagementException(String.format("Failed to delete modules, details: %s", response));
    }
  }

  private HttpResponse doGet(String resource)
    throws DatasetManagementException, UnauthorizedException {
    return doRequest(remoteClient.requestBuilder(HttpMethod.GET, resource));
  }

  private HttpResponse doPut(String resource, String body)
    throws DatasetManagementException, UnauthorizedException {
    return doRequest(remoteClient.requestBuilder(HttpMethod.PUT, resource).withBody(body));
  }

  private HttpResponse doPost(String resource)
    throws DatasetManagementException, UnauthorizedException {
    return doRequest(remoteClient.requestBuilder(HttpMethod.POST, resource));
  }

  private HttpResponse doPost(String resource, String body)
    throws DatasetManagementException, UnauthorizedException {
    return doRequest(remoteClient.requestBuilder(HttpMethod.POST, resource).withBody(body));
  }

  private HttpResponse doDelete(String resource)
    throws DatasetManagementException, UnauthorizedException {
    return doRequest(remoteClient.requestBuilder(HttpMethod.DELETE, resource));
  }

  private HttpResponse doRequest(HttpRequest.Builder requestBuilder)
    throws DatasetManagementException, UnauthorizedException {
    HttpRequest request = addUserIdHeader(requestBuilder).build();
    try {
      LOG.trace("Executing {} {}", request.getMethod(), request.getURL().getPath());
      HttpResponse response = remoteClient.execute(request);
      LOG.trace("Executed {} {}", request.getMethod(), request.getURL().getPath());
      return response;
    } catch (ServiceUnavailableException e) { // thrown by RemoteClient in case of ConnectException
      logThreadDump();
      LOG.trace("Caught exception for {} {}", request.getMethod(), request.getURL().getPath(), e);
      throw e;
    } catch (SocketTimeoutException e) { // passed through by RemoteClient
      logThreadDump();
      LOG.trace("Caught exception for {} {}", request.getMethod(), request.getURL().getPath(), e);
      throw new DatasetManagementException(remoteClient.createErrorMessage(request, null), e);
    } catch (IOException e) { // other network exceptions
      LOG.trace("Caught exception for {} {}", request.getMethod(), request.getURL().getPath(), e);
      throw new DatasetManagementException(remoteClient.createErrorMessage(request, null), e);
    } catch (Throwable e) { // anything unexpected
      LOG.trace("Caught exception for {} {}", request.getMethod(), request.getURL().getPath(), e);
      throw e;
    }
  }

  public static String getCallerId(io.netty.handler.codec.http.HttpRequest request) {
    String callerId = request.headers().get("callerId");
    return callerId != null ? callerId : "unknown http client";
  }

  private HttpRequest.Builder addUserIdHeader(HttpRequest.Builder builder) throws DatasetManagementException {
    if (LOG.isTraceEnabled()) {
      builder = builder.addHeader("callerId", Thread.currentThread().getName());
    }
    if (!securityEnabled || !authorizationEnabled) {
      return builder;
    }

    String currUserShortName;
    try {
      currUserShortName = UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (IOException e) {
      throw new DatasetManagementException("Unable to get the current user", e);
    }

    // If the request originated from the router and was forwarded to any service other than dataset service, before
    // going to dataset service via dataset service client, the userId could be set in the SecurityRequestContext.
    // e.g. deploying an app that contains a dataset.
    // For user datasets, if a dataset call originates from a program runtime, then find the userId from
    // UserGroupInformation#getCurrentUser()
    String userId = authenticationContext.getPrincipal().getName();
    if (NamespaceId.SYSTEM.equals(namespaceId)) {
      // For getting a system dataset like MDS, use the system principal, if the current user is the same as the
      // CDAP kerberos principal. If a user tries to access a system dataset from an app, either:
      // 1. The request will go through if the user is impersonating as the cdap principal - which means that
      // impersonation has specifically been configured in this namespace to use the cdap principal; or
      // 2. The request will fail, if kerberos is enabled and the user is impersonating as a non-cdap user; or
      // 3. The request will go through, if kerberos is disabled, since the impersonating user will be cdap

      // we compare short name, because in some YARN containers launched by CDAP, the current username isn't the full
      // configured principal
      if (!kerberosEnabled || currUserShortName.equals(masterShortUserName)) {
        LOG.trace("Accessing dataset in system namespace using the system principal because the current user " +
                    "{} is the same as the CDAP master user {}.",
                  currUserShortName, masterShortUserName);
        userId = currUserShortName;
      }
    }
    return builder.addHeader(Constants.Security.Headers.USER_ID, userId);
  }

  private static void logThreadDump() {
    if (LOG.isTraceEnabled()) {
      StringBuilder sb = new StringBuilder("Timeout while making request to dataset service. Thread dump follows:\n");
      ThreadMXBean bean = ManagementFactory.getThreadMXBean();
      for (ThreadInfo threadInfo : bean.dumpAllThreads(true, true)) {
        append(sb, threadInfo);
        sb.append("\n");
      }
      LOG.trace(sb.toString());
    }
  }

  @SuppressWarnings("StringConcatenationInsideStringBufferAppend")
  private static void append(StringBuilder sb, ThreadInfo threadInfo) {
    sb.append("\"" + threadInfo.getThreadName() + "\"" +
                " Id=" + threadInfo.getThreadId() + " " +
                threadInfo.getThreadState());
    if (threadInfo.getLockName() != null) {
      sb.append(" on " + threadInfo.getLockName());
    }
    if (threadInfo.getLockOwnerName() != null) {
      sb.append(" owned by \"" + threadInfo.getLockOwnerName() +
                  "\" Id=" + threadInfo.getLockOwnerId());
    }
    if (threadInfo.isSuspended()) {
      sb.append(" (suspended)");
    }
    if (threadInfo.isInNative()) {
      sb.append(" (in native)");
    }
    sb.append('\n');
    int i = 0;
    StackTraceElement[] stackTrace = threadInfo.getStackTrace();
    for (; i < stackTrace.length && i < 1000; i++) {
      StackTraceElement ste = stackTrace[i];
      sb.append("\tat " + ste.toString());
      sb.append('\n');
      if (i == 0 && threadInfo.getLockInfo() != null) {
        Thread.State ts = threadInfo.getThreadState();
        switch (ts) {
          case BLOCKED:
            sb.append("\t-  blocked on " + threadInfo.getLockInfo());
            sb.append('\n');
            break;
          case WAITING:
            sb.append("\t-  waiting on " + threadInfo.getLockInfo());
            sb.append('\n');
            break;
          case TIMED_WAITING:
            sb.append("\t-  waiting on " + threadInfo.getLockInfo());
            sb.append('\n');
            break;
          default:
        }
      }

      for (MonitorInfo mi : threadInfo.getLockedMonitors()) {
        if (mi.getLockedStackDepth() == i) {
          sb.append("\t-  locked " + mi);
          sb.append('\n');
        }
      }
    }
    if (i < stackTrace.length) {
      sb.append("\t...");
      sb.append('\n');
    }

    LockInfo[] locks = threadInfo.getLockedSynchronizers();
    if (locks.length > 0) {
      sb.append("\n\tNumber of locked synchronizers = " + locks.length);
      sb.append('\n');
      for (LockInfo li : locks) {
        sb.append("\t- " + li);
        sb.append('\n');
      }
    }
    sb.append('\n');
  }
}
