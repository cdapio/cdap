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

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.common.ConflictException;
import co.cask.cdap.common.HandlerException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.EndpointStrategy;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.common.service.UncaughtExceptionIdleService;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.cdap.proto.Id;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequestConfig;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import com.google.common.base.Charsets;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Executes Dataset operations by querying a {@link DatasetOpExecutorService} via REST.
 */
public abstract class RemoteDatasetOpExecutor extends UncaughtExceptionIdleService implements DatasetOpExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteDatasetOpExecutor.class);

  private static final Gson GSON = new Gson();

  private final CConfiguration cConf;
  private final Supplier<EndpointStrategy> endpointStrategySupplier;
  private final HttpRequestConfig httpRequestConfig;

  @Inject
  public RemoteDatasetOpExecutor(CConfiguration cConf, final DiscoveryServiceClient discoveryClient) {
    this.cConf = cConf;
    this.endpointStrategySupplier = Suppliers.memoize(new Supplier<EndpointStrategy>() {
      @Override
      public EndpointStrategy get() {
        return new RandomEndpointStrategy(discoveryClient.discover(Constants.Service.DATASET_EXECUTOR));
      }
    });
    int httpTimeoutMs = cConf.getInt(Constants.HTTP_CLIENT_TIMEOUT_MS);
    this.httpRequestConfig = new HttpRequestConfig(httpTimeoutMs, httpTimeoutMs);
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
  public boolean exists(Id.DatasetInstance datasetInstanceId) throws Exception {
    return (Boolean) executeAdminOp(datasetInstanceId, "exists").getResult();
  }

  @Override
  public DatasetSpecification create(Id.DatasetInstance datasetInstanceId, DatasetTypeMeta typeMeta,
                                     DatasetProperties props) throws Exception {

    InternalDatasetCreationParams creationParams = new InternalDatasetCreationParams(typeMeta, props);
    HttpRequest request = HttpRequest.post(resolve(datasetInstanceId, "create"))
      .withBody(GSON.toJson(creationParams))
      .build();
    HttpResponse response = HttpRequests.execute(request, httpRequestConfig);
    verifyResponse(response);

    return ObjectResponse.fromJsonBody(response, DatasetSpecification.class).getResponseObject();
  }

  @Override
  public DatasetSpecification update(Id.DatasetInstance datasetInstanceId, DatasetTypeMeta typeMeta,
                                     DatasetProperties props, DatasetSpecification existing) throws Exception {

    InternalDatasetCreationParams updateParams = new InternalDatasetUpdateParams(typeMeta, existing, props);
    HttpRequest request = HttpRequest.post(resolve(datasetInstanceId, "update"))
      .withBody(GSON.toJson(updateParams))
      .build();
    HttpResponse response = HttpRequests.execute(request, httpRequestConfig);
    verifyResponse(response);

    return ObjectResponse.fromJsonBody(response, DatasetSpecification.class).getResponseObject();
  }

  @Override
  public void drop(Id.DatasetInstance datasetInstanceId, DatasetTypeMeta typeMeta, DatasetSpecification spec)
    throws Exception {
    InternalDatasetDropParams dropParams = new InternalDatasetDropParams(typeMeta, spec);
    HttpRequest request = HttpRequest.post(resolve(datasetInstanceId, "drop"))
      .withBody(GSON.toJson(dropParams)).build();
    HttpResponse response = HttpRequests.execute(request, httpRequestConfig);
    verifyResponse(response);
  }

  @Override
  public void truncate(Id.DatasetInstance datasetInstanceId) throws Exception {
    executeAdminOp(datasetInstanceId, "truncate");
  }

  @Override
  public void upgrade(Id.DatasetInstance datasetInstanceId) throws Exception {
    executeAdminOp(datasetInstanceId, "upgrade");
  }

  private DatasetAdminOpResponse executeAdminOp(Id.DatasetInstance datasetInstanceId, String opName)
    throws IOException, HandlerException, ConflictException {

    HttpResponse httpResponse = HttpRequests.execute(HttpRequest.post(resolve(datasetInstanceId, opName)).build(),
                                                      httpRequestConfig);
    verifyResponse(httpResponse);

    return GSON.fromJson(new String(httpResponse.getResponseBody()), DatasetAdminOpResponse.class);
  }

  private URL resolve(Id.DatasetInstance datasetInstanceId, String opName) throws MalformedURLException {
    return resolve(String.format("namespaces/%s/data/datasets/%s/admin/%s", datasetInstanceId.getNamespaceId(),
                                 datasetInstanceId.getId(), opName));
  }

  private URL resolve(String path) throws MalformedURLException {
    Discoverable endpoint = endpointStrategySupplier.get().pick(2L, TimeUnit.SECONDS);
    if (endpoint == null) {
      throw new IllegalStateException("No endpoint for " + Constants.Service.DATASET_EXECUTOR);
    }
    InetSocketAddress addr = endpoint.getSocketAddress();
    return new URL(String.format("http://%s:%s%s/%s",
                         addr.getHostName(), addr.getPort(),
                         Constants.Gateway.API_VERSION_3,
                         path));
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

  @Override
  protected Logger getUncaughtExceptionLogger() {
    return LOG;
  }
}
