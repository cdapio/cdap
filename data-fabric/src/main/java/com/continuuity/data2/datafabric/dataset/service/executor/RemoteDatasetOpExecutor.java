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

package com.continuuity.data2.datafabric.dataset.service.executor;

import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.discovery.TimeLimitEndpointStrategy;
import com.continuuity.common.exception.HandlerException;
import com.continuuity.common.http.HttpRequest;
import com.continuuity.common.http.HttpRequests;
import com.continuuity.common.http.HttpResponse;
import com.continuuity.common.http.ObjectResponse;
import com.continuuity.proto.DatasetTypeMeta;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Executes Dataset operations by querying a {@link DatasetOpExecutorService} via REST.
 */
public abstract class RemoteDatasetOpExecutor extends AbstractIdleService implements DatasetOpExecutor {

  private static final Gson GSON = new Gson();

  private final Supplier<EndpointStrategy> endpointStrategySupplier;

  @Inject
  public RemoteDatasetOpExecutor(final DiscoveryServiceClient discoveryClient) {
    this.endpointStrategySupplier = Suppliers.memoize(new Supplier<EndpointStrategy>() {
      @Override
      public EndpointStrategy get() {
        return new TimeLimitEndpointStrategy(
          new RandomEndpointStrategy(
            discoveryClient.discover(Constants.Service.DATASET_EXECUTOR)), 10L, TimeUnit.SECONDS);
      }
    });
  }

  @Override
  public boolean exists(String instanceName) throws Exception {
    return (Boolean) executeAdminOp(instanceName, "exists").getResult();
  }

  @Override
  public DatasetSpecification create(String instanceName, DatasetTypeMeta typeMeta, DatasetProperties props)
    throws Exception {


    Map<String, String> headers = ImmutableMap.of("instance-props", GSON.toJson(props),
                                                  "type-meta", GSON.toJson(typeMeta));
    HttpRequest request = HttpRequest.post(resolve(instanceName, "create")).addHeaders(headers).build();
    HttpResponse response = HttpRequests.execute(request);
    verifyResponse(response);

    return ObjectResponse.fromJsonBody(response, DatasetSpecification.class).getResponseObject();
  }

  @Override
  public void drop(DatasetSpecification spec, DatasetTypeMeta typeMeta) throws Exception {
    Map<String, String> headers = ImmutableMap.of("instance-spec", GSON.toJson(spec),
                                                  "type-meta", GSON.toJson(typeMeta));
    HttpRequest request = HttpRequest.post(resolve(spec.getName(), "drop")).addHeaders(headers).build();
    HttpResponse response = HttpRequests.execute(request);
    verifyResponse(response);
  }

  @Override
  public void truncate(String instanceName) throws Exception {
    executeAdminOp(instanceName, "truncate");
  }

  @Override
  public void upgrade(String instanceName) throws Exception {
    executeAdminOp(instanceName, "upgrade");
  }

  private DatasetAdminOpResponse executeAdminOp(String instanceName, String opName)
    throws IOException, HandlerException {

    HttpResponse httpResponse = HttpRequests.execute(HttpRequest.post(resolve(instanceName, opName)).build());
    if (httpResponse.getResponseCode() != 200) {
      throw new HandlerException(HttpResponseStatus.valueOf(httpResponse.getResponseCode()),
                                 httpResponse.getResponseMessage());
    }

    return GSON.fromJson(new String(httpResponse.getResponseBody()), DatasetAdminOpResponse.class);
  }

  private URL resolve(String instanceName, String opName) throws MalformedURLException {
    return resolve(String.format("datasets/%s/admin/%s", instanceName, opName));
  }

  private URL resolve(String path) throws MalformedURLException {
    InetSocketAddress addr = this.endpointStrategySupplier.get().pick().getSocketAddress();
    return new URL(String.format("http://%s:%s%s/data/%s",
                         addr.getHostName(), addr.getPort(),
                         Constants.Gateway.GATEWAY_VERSION,
                         path));
  }

  private void verifyResponse(HttpResponse httpResponse) {
    if (httpResponse.getResponseCode() != 200) {
      throw new HandlerException(HttpResponseStatus.valueOf(httpResponse.getResponseCode()),
                                 httpResponse.getResponseMessage());
    }
  }
}
