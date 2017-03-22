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
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.EndpointStrategy;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.common.http.DefaultHttpRequestConfig;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequestConfig;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import com.google.common.base.Charsets;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
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
  private final Supplier<EndpointStrategy> endpointStrategySupplier;
  private final HttpRequestConfig httpRequestConfig;
  private final AuthenticationContext authenticationContext;

  @Inject
  RemoteDatasetOpExecutor(CConfiguration cConf, final DiscoveryServiceClient discoveryClient,
                          AuthenticationContext authenticationContext) {
    this.cConf = cConf;
    this.authenticationContext = authenticationContext;
    this.endpointStrategySupplier = Suppliers.memoize(new Supplier<EndpointStrategy>() {
      @Override
      public EndpointStrategy get() {
        return new RandomEndpointStrategy(discoveryClient.discover(Constants.Service.DATASET_EXECUTOR));
      }
    });
    this.httpRequestConfig = new DefaultHttpRequestConfig(false);
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
          public Boolean call() throws Exception {
            return endpointStrategySupplier.get().pick() != null;
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
    HttpRequest.Builder builder = HttpRequest.post(resolve(datasetInstanceId, opName));
    if (body != null) {
      builder.withBody(body);
    }
    String userId = authenticationContext.getPrincipal().getName();
    if (userId != null) {
      builder.addHeader(Constants.Security.Headers.USER_ID, userId);
    }
    HttpResponse httpResponse = HttpRequests.execute(builder.build(), httpRequestConfig);
    verifyResponse(httpResponse);
    return httpResponse;
  }

  private URL resolve(DatasetId datasetInstanceId, String opName) throws MalformedURLException {
    return resolve(String.format("namespaces/%s/data/datasets/%s/admin/%s", datasetInstanceId.getNamespace(),
                                 datasetInstanceId.getEntityName(), opName));
  }

  private URL resolve(String path) throws MalformedURLException {
    Discoverable endpoint = endpointStrategySupplier.get().pick(2L, TimeUnit.SECONDS);
    if (endpoint == null) {
      throw new IllegalStateException("No endpoint for " + Constants.Service.DATASET_EXECUTOR);
    }
    String scheme = Arrays.equals(Constants.Security.SSL_URI_SCHEME.getBytes(), endpoint.getPayload()) ?
      Constants.Security.SSL_URI_SCHEME : Constants.Security.URI_SCHEME;
    InetSocketAddress address = endpoint.getSocketAddress();
    return new URL(String.format("%s%s:%s%s/%s", scheme, address.getHostName(), address.getPort(),
                                 Constants.Gateway.API_VERSION_3, path));
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
