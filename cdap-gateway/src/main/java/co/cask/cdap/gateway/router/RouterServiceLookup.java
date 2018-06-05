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

package co.cask.cdap.gateway.router;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.EndpointStrategy;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.common.service.ServiceDiscoverable;
import co.cask.cdap.gateway.discovery.RouteFallbackStrategy;
import co.cask.cdap.gateway.discovery.UserServiceEndpointStrategy;
import co.cask.cdap.route.store.RouteStore;
import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpRequest;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Port -> service lookup.
 */
public class RouterServiceLookup {
  private static final Logger LOG = LoggerFactory.getLogger(RouterServiceLookup.class);

  private final DiscoveryServiceClient discoveryServiceClient;
  private final LoadingCache<RouteDestination, EndpointStrategy> discoverableCache;
  private final RouterPathLookup routerPathLookup;
  private final RouteStore routeStore;
  private final RouteFallbackStrategy fallbackStrategy;

  @Inject
  RouterServiceLookup(CConfiguration cConf, DiscoveryServiceClient discoveryServiceClient,
                      RouterPathLookup routerPathLookup, RouteStore routeStore) {
    this.discoveryServiceClient = discoveryServiceClient;
    this.routerPathLookup = routerPathLookup;
    this.discoverableCache = CacheBuilder.newBuilder()
      .expireAfterAccess(1, TimeUnit.HOURS)
      .build(new CacheLoader<RouteDestination, EndpointStrategy>() {
        @Override
        public EndpointStrategy load(RouteDestination key) throws Exception {
          return loadCache(key);
        }
      });
    this.routeStore = routeStore;
    this.fallbackStrategy = RouteFallbackStrategy.valueOfRouteFallbackStrategy(
      cConf.get(Constants.Router.ROUTER_USERSERVICE_FALLBACK_STRAGEY));
  }

  /**
   * Returns the discoverable mapped to the given port.
   *
   * @param httpRequest supplies the header information for the lookup.
   * @return instance of EndpointStrategy if available null otherwise.
   */
  @Nullable
  public EndpointStrategy getDiscoverable(HttpRequest httpRequest) {
    // Normalize the path once and strip off any query string. Just keep the URI path.
    String path = URI.create(httpRequest.uri()).normalize().getPath();

    try {
      // Check if the requested path shouldn't be routed (internal URL).
      RouteDestination destService = routerPathLookup.getRoutingService(path, httpRequest);
      if (destService == null || Strings.isNullOrEmpty(destService.getServiceName())
        || destService.getServiceName().equals(Constants.Router.DONT_ROUTE_SERVICE)) {
        return null;
      }

      LOG.trace("Request was routed from {} to: {}", path, destService);

      return discoverableCache.get(destService);
    } catch (ExecutionException e) {
      return null;
    }
  }

  private EndpointStrategy loadCache(RouteDestination cacheKey) throws Exception {
    EndpointStrategy endpointStrategy = discover(cacheKey);

    if (endpointStrategy.pick() == null) {
      String message = String.format("No discoverable endpoints found for service %s", cacheKey);
      LOG.debug(message);
      throw new Exception(message);
    }

    return endpointStrategy;
  }

  private EndpointStrategy discover(RouteDestination routeDestination) {
    LOG.debug("Looking up service name {}", routeDestination);
    // If its a user service, then use UserServiceEndpointStrategy Strategy
    String serviceName = routeDestination.getServiceName();
    ServiceDiscovered serviceDiscovered = discoveryServiceClient.discover(serviceName);

    EndpointStrategy endpointStrategy = ServiceDiscoverable.isUserService(serviceName) ?
      new UserServiceEndpointStrategy(serviceDiscovered, routeStore, ServiceDiscoverable.getId(serviceName),
                                      fallbackStrategy, routeDestination.getVersion()) :
      new RandomEndpointStrategy(serviceDiscovered);
    if (endpointStrategy.pick(300L, TimeUnit.MILLISECONDS) == null) {
      LOG.debug("Discoverable endpoint {} not found", routeDestination);
    }
    return endpointStrategy;
  }
}
