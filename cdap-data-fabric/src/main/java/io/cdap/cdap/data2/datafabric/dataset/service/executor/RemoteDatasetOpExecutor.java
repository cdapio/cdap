/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.data2.datafabric.dataset.service.executor;

import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.common.ConflictException;
import io.cdap.cdap.common.HandlerException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.proto.DatasetTypeMeta;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import io.cdap.common.http.ObjectResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import javax.annotation.Nullable;
import javax.ws.rs.core.HttpHeaders;

/**
 * Executes Dataset operations by querying a {@link DatasetOpExecutorService} via REST.
 */
public class RemoteDatasetOpExecutor implements DatasetOpExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteDatasetOpExecutor.class);

  private static final Gson GSON = new Gson();

  private final RemoteClient remoteClient;
  private final AuthenticationContext authenticationContext;
  private boolean addAdditionalHeader;

  @Inject
  public RemoteDatasetOpExecutor(DiscoveryServiceClient discoveryClient, AuthenticationContext authenticationContext) {
    this.authenticationContext = authenticationContext;
    this.remoteClient = new RemoteClient(discoveryClient, Constants.Service.DATASET_EXECUTOR,
                                         new DefaultHttpRequestConfig(false), Constants.Gateway.API_VERSION_3);
    if (discoveryClient.getClass().getName().contains("LauncherDiscoveryService")) {
      addAdditionalHeader = true;
    }
  }

  @Override
  public boolean exists(DatasetId datasetInstanceId) throws Exception {
    return (Boolean) executeAdminOp(datasetInstanceId, "exists").getResult();
  }

  @Override
  public DatasetCreationResponse create(DatasetId datasetInstanceId, DatasetTypeMeta typeMeta,
                                        DatasetProperties props) throws Exception {
    InternalDatasetCreationParams creationParams = new InternalDatasetCreationParams(typeMeta, props);
    HttpResponse response = doRequest(datasetInstanceId, "create", GSON.toJson(creationParams));
    return ObjectResponse.fromJsonBody(response, DatasetCreationResponse.class).getResponseObject();
  }

  @Override
  public DatasetCreationResponse update(DatasetId datasetInstanceId, DatasetTypeMeta typeMeta,
                                        DatasetProperties props, DatasetSpecification existing) throws Exception {
    InternalDatasetCreationParams updateParams = new InternalDatasetUpdateParams(typeMeta, existing, props);
    HttpResponse response = doRequest(datasetInstanceId, "update", GSON.toJson(updateParams));
    return ObjectResponse.fromJsonBody(response, DatasetCreationResponse.class).getResponseObject();
  }

  @Override
  public void drop(DatasetId datasetInstanceId, DatasetTypeMeta typeMeta, DatasetSpecification spec)
    throws Exception {
    InternalDatasetDropParams dropParams = new InternalDatasetDropParams(typeMeta, spec);
    doRequest(datasetInstanceId, "drop", GSON.toJson(dropParams));
  }

  @Override
  public void truncate(DatasetId datasetInstanceId) throws Exception {
    executeAdminOp(datasetInstanceId, "truncate");
  }

  @Override
  public void upgrade(DatasetId datasetInstanceId) throws Exception {
    executeAdminOp(datasetInstanceId, "upgrade");
  }

  private DatasetAdminOpResponse executeAdminOp(DatasetId datasetInstanceId, String opName)
    throws IOException, HandlerException, ConflictException {
    HttpResponse httpResponse = doRequest(datasetInstanceId, opName, null);
    return GSON.fromJson(Bytes.toString(httpResponse.getResponseBody()), DatasetAdminOpResponse.class);
  }

  private HttpResponse doRequest(DatasetId datasetInstanceId, String opName,
                                 @Nullable String body) throws IOException, ConflictException {
    String path = String.format("namespaces/%s/data/datasets/%s/admin/%s", datasetInstanceId.getNamespace(),
                                datasetInstanceId.getEntityName(), opName);
    LOG.trace("executing POST on {} with body {}", path, body);
    try {
      HttpRequest.Builder builder = remoteClient.requestBuilder(HttpMethod.POST, path);
      if (body != null) {
        builder.withBody(body);
      }
      if (addAdditionalHeader) {
        try {
          LOG.info("Additional header is true..");
          URL url = new URL("http://metadata/computeMetadata/v1/instance/service-accounts/default/token");
          HttpResponse response = HttpRequests.execute(io.cdap.common.http.HttpRequest.get(url)
                                                         .addHeader("Metadata-Flavor", "Google").build(),
                                                       new DefaultHttpRequestConfig(false));

          LOG.info("### response from auth server json {}", response.getResponseBodyAsString());
          JsonObject jobj = new Gson().fromJson(response.getResponseBodyAsString(), JsonObject.class);
          String accessToken = jobj.get("access_token").getAsString();

          builder.addHeader(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken);
        } catch (Exception e) {
          LOG.error("### error while getting token: {}", e.getMessage(), e);
        }
      }
      String userId = authenticationContext.getPrincipal().getName();
      if (userId != null) {
        builder.addHeader(Constants.Security.Headers.USER_ID, userId);
      }
      HttpResponse httpResponse = remoteClient.execute(builder.build());
      LOG.trace("executed POST on {} with body {}: {}", path, body, httpResponse.getResponseCode());
      verifyResponse(httpResponse);
      return httpResponse;
    } catch (Exception e) {
      LOG.trace("Caught exception for POST on {} with body {}", path, body, e);
      throw e;
    }
  }

  private void verifyResponse(HttpResponse httpResponse) throws ConflictException {
    if (httpResponse.getResponseCode() == 409) {
      throw new ConflictException(httpResponse.getResponseBodyAsString(Charsets.UTF_8));
    }
    if (httpResponse.getResponseCode() != 200) {
      throw new HandlerException(HttpResponseStatus.valueOf(httpResponse.getResponseCode()),
                                 httpResponse.getResponseBodyAsString(Charsets.UTF_8));
    }
  }
}
