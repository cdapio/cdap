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

package co.cask.cdap.data2.datafabric.dataset.service.executor;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.common.ConflictException;
import co.cask.cdap.common.HandlerException;
import co.cask.cdap.common.ServiceUnavailableException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.http.DefaultHttpRequestConfig;
import co.cask.cdap.common.internal.remote.RemoteClient;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import com.google.common.base.Charsets;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;

/**
 * Executes Dataset operations by querying a {@link DatasetOpExecutorService} via REST.
 */
public abstract class RemoteDatasetOpExecutor extends AbstractIdleService implements DatasetOpExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteDatasetOpExecutor.class);

  private static final Gson GSON = new Gson();

  private final CConfiguration cConf;
  private final RemoteClient remoteClient;
  private final AuthenticationContext authenticationContext;

  @Inject
  RemoteDatasetOpExecutor(CConfiguration cConf, final DiscoveryServiceClient discoveryClient,
                          AuthenticationContext authenticationContext) {
    this.cConf = cConf;
    this.authenticationContext = authenticationContext;
    this.remoteClient = new RemoteClient(discoveryClient, Constants.Service.DATASET_EXECUTOR,
                                         new DefaultHttpRequestConfig(false), Constants.Gateway.API_VERSION_3);
  }

  @Override
  protected void startUp() throws Exception {
    // wait for dataset executor to be discoverable
    LOG.info("Starting DatasetOpExecutor.");
    int timeout = cConf.getInt(Constants.Startup.STARTUP_SERVICE_TIMEOUT);
    if (timeout > 0) {
      try {
        Tasks.waitFor(true, new Callable<Boolean>() {
          @Override
          public Boolean call() {
            try {
              remoteClient.resolve("");
            } catch (ServiceUnavailableException e) {
              return false;
            }
            return true;
          }
        }, timeout, TimeUnit.SECONDS, Math.min(timeout, Math.max(10, timeout / 10)), TimeUnit.SECONDS);
        LOG.info("DatasetOpExecutor started.");
      } catch (TimeoutException e) {
        // its not a nice message... throw one with a better message
        throw new TimeoutException(String.format("Timed out waiting to discover the %s service. " +
                                                   "Check the container logs then try restarting the service.",
                                                 Constants.Service.DATASET_EXECUTOR));
      } catch (InterruptedException e) {
        throw new RuntimeException(String.format("Interrupted while waiting to discover the %s service.",
                                                 Constants.Service.DATASET_EXECUTOR));
      } catch (ExecutionException e) {
        throw new RuntimeException(String.format("Error while waiting to discover the %s service.",
                                                 Constants.Service.DATASET_EXECUTOR), e);
      }
    }
  }

  @Override
  public boolean exists(DatasetId datasetInstanceId) throws Exception {
    return (Boolean) executeAdminOp(datasetInstanceId, "exists", null).getResult();
  }

  @Override
  public DatasetSpecification create(DatasetId datasetInstanceId, DatasetTypeMeta typeMeta,
                                     DatasetProperties props) throws Exception {
    InternalDatasetCreationParams creationParams = new InternalDatasetCreationParams(typeMeta, props);
    HttpResponse response = doRequest(datasetInstanceId, "create", GSON.toJson(creationParams));
    return ObjectResponse.fromJsonBody(response, DatasetSpecification.class).getResponseObject();
  }

  @Override
  public DatasetSpecification update(DatasetId datasetInstanceId, DatasetTypeMeta typeMeta,
                                     DatasetProperties props, DatasetSpecification existing) throws Exception {
    InternalDatasetCreationParams updateParams = new InternalDatasetUpdateParams(typeMeta, existing, props);
    HttpResponse response = doRequest(datasetInstanceId, "update", GSON.toJson(updateParams));
    return ObjectResponse.fromJsonBody(response, DatasetSpecification.class).getResponseObject();
  }

  @Override
  public void drop(DatasetId datasetInstanceId, DatasetTypeMeta typeMeta, DatasetSpecification spec)
    throws Exception {
    InternalDatasetDropParams dropParams = new InternalDatasetDropParams(typeMeta, spec);
    doRequest(datasetInstanceId, "drop", GSON.toJson(dropParams));
  }

  @Override
  public void truncate(DatasetId datasetInstanceId) throws Exception {
    executeAdminOp(datasetInstanceId, "truncate", null);
  }

  @Override
  public void upgrade(DatasetId datasetInstanceId) throws Exception {
    executeAdminOp(datasetInstanceId, "upgrade", null);
  }

  private DatasetAdminOpResponse executeAdminOp(
    DatasetId datasetInstanceId, String opName,
    @Nullable String body) throws IOException, HandlerException, ConflictException {
    HttpResponse httpResponse = doRequest(datasetInstanceId, opName, body);
    return GSON.fromJson(Bytes.toString(httpResponse.getResponseBody()), DatasetAdminOpResponse.class);
  }

  private HttpResponse doRequest(DatasetId datasetInstanceId, String opName,
                                 @Nullable String body) throws IOException, ConflictException {
    String path = String.format("namespaces/%s/data/datasets/%s/admin/%s", datasetInstanceId.getNamespace(),
                                datasetInstanceId.getEntityName(), opName);
    HttpRequest.Builder builder = remoteClient.requestBuilder(HttpMethod.POST, path);
    if (body != null) {
      builder.withBody(body);
    }
    String userId = authenticationContext.getPrincipal().getName();
    if (userId != null) {
      builder.addHeader(Constants.Security.Headers.USER_ID, userId);
    }
    HttpResponse httpResponse = remoteClient.execute(builder.build());
    verifyResponse(httpResponse);
    return httpResponse;
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
