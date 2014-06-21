package com.continuuity.explore.client;

import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.discovery.TimeLimitEndpointStrategy;
import com.continuuity.common.http.HttpResponse;
import com.continuuity.explore.service.Explore;
import com.continuuity.explore.service.ExploreException;
import com.continuuity.explore.service.Handle;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.inject.Inject;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import static com.continuuity.common.conf.Constants.Service;

/**
 * An Explore Client that talks to a server implementing {@link Explore} over HTTP,
 * and that uses discovery to find the endpoints.
 */
public class InternalAsyncExploreClient extends AbstractAsyncExploreClient {
  private static final Logger LOG = LoggerFactory.getLogger(InternalAsyncExploreClient.class);

  private final Supplier<EndpointStrategy> endpointStrategySupplier;

  @Inject
  public InternalAsyncExploreClient(final DiscoveryServiceClient discoveryClient) {
    this.endpointStrategySupplier = Suppliers.memoize(new Supplier<EndpointStrategy>() {
      @Override
      public EndpointStrategy get() {
        return new TimeLimitEndpointStrategy(
          new RandomEndpointStrategy(
            discoveryClient.discover(Service.EXPLORE_HTTP_USER_SERVICE)), 3L, TimeUnit.SECONDS);
      }
    });
  }

  @Override
  protected InetSocketAddress getExploreServiceAddress() {
    EndpointStrategy endpointStrategy = this.endpointStrategySupplier.get();
    if (endpointStrategy == null || endpointStrategy.pick() == null) {
      String message = String.format("Cannot discover service %s", Service.EXPLORE_HTTP_USER_SERVICE);
      LOG.error(message);
      throw new RuntimeException(message);
    }

    return endpointStrategy.pick().getSocketAddress();
  }
}
