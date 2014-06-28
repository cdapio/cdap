package com.continuuity.data2.datafabric.dataset.service.executor;

import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.discovery.TimeLimitEndpointStrategy;
import com.continuuity.common.exception.HandlerException;
import com.continuuity.common.http.HttpRequests;
import com.continuuity.common.http.HttpResponse;
import com.continuuity.data2.datafabric.dataset.type.DatasetTypeMeta;
import com.google.common.base.Charsets;
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

    HttpResponse httpResponse =
      HttpRequests.post(resolve(instanceName, "create"), "",
                        ImmutableMap.of("instance-props", GSON.toJson(props),
                                        "type-meta", GSON.toJson(typeMeta)));
    verifyResponse(httpResponse);

    return GSON.fromJson(new String(httpResponse.getResponseBody(), Charsets.UTF_8), DatasetSpecification.class);
  }

  @Override
  public void drop(DatasetSpecification spec, DatasetTypeMeta typeMeta) throws Exception {
    HttpResponse httpResponse =
      HttpRequests.post(resolve(spec.getName(), "drop"), "",
                        ImmutableMap.of("instance-spec", GSON.toJson(spec),
                                        "type-meta", GSON.toJson(typeMeta)));
    verifyResponse(httpResponse);
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

    HttpResponse httpResponse = HttpRequests.post(resolve(instanceName, opName));
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
