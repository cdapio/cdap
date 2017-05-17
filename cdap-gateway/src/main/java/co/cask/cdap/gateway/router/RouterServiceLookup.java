/*
 * Copyright © 2014 Cask Data, Inc.
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
import co.cask.cdap.common.utils.Networks;
import co.cask.cdap.gateway.discovery.RouteFallbackStrategy;
import co.cask.cdap.gateway.discovery.UserServiceEndpointStrategy;
import co.cask.cdap.route.store.RouteStore;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Port -> service lookup.
 */
public class RouterServiceLookup {
  private static final Logger LOG = LoggerFactory.getLogger(RouterServiceLookup.class);
  private static final String DEFAULT_SERVICE_NAME = "default";

  private final AtomicReference<Map<Integer, String>> serviceMapRef =
    new AtomicReference<Map<Integer, String>>(ImmutableMap.<Integer, String>of());

  private final DiscoveryServiceClient discoveryServiceClient;
  private final LoadingCache<CacheKey, EndpointStrategy> discoverableCache;
  private final RouterPathLookup routerPathLookup;
  private final RouteStore routeStore;
  private final RouteFallbackStrategy fallbackStrategy;

  @Inject
  public RouterServiceLookup(CConfiguration cConf, DiscoveryServiceClient discoveryServiceClient,
                             RouterPathLookup routerPathLookup, RouteStore routeStore) {
    this.discoveryServiceClient = discoveryServiceClient;
    this.routerPathLookup = routerPathLookup;
    this.discoverableCache = CacheBuilder.newBuilder()
      .expireAfterAccess(1, TimeUnit.HOURS)
      .build(new CacheLoader<CacheKey, EndpointStrategy>() {
        @Override
        public EndpointStrategy load(CacheKey key) throws Exception {
          return loadCache(key);
        }
      });
    this.routeStore = routeStore;
    this.fallbackStrategy = RouteFallbackStrategy.valueOfRouteFallbackStrategy(
      cConf.get(Constants.Router.ROUTER_USERSERVICE_FALLBACK_STRAGEY));
  }

  /**
   * Lookup service name given port.
   *
   * @param port port to lookup.
   * @return service name based on port.
   */
  public String getService(int port) {
    return serviceMapRef.get().get(port);
  }

  /**
   * @return the port to service name map for all services.
   */
  public Map<Integer, String> getServiceMap() {
    return ImmutableMap.copyOf(serviceMapRef.get());
  }

  /**
   * Returns the discoverable mapped to the given port.
   *
   * @param port port to lookup.
   * @param httpRequest supplies the header information for the lookup.
   * @return instance of EndpointStrategy if available null otherwise.
   */
  public EndpointStrategy getDiscoverable(int port, HttpRequest httpRequest) {
    //Get the service based on Port.
    final String service = serviceMapRef.get().get(port);
    if (service == null) {
      LOG.debug("No service found for port {}", port);
      return null;
    }

    // Normalize the path once and strip off any query string. Just keep the URI path.
    String path = URI.create(httpRequest.getUri()).normalize().getPath();
    String host = httpRequest.getHeader(HttpHeaders.Names.HOST);

    if (host == null) {
      LOG.debug("Cannot find host header for service {} on port {}", service, port);
      return null;
    }

    try {
      // Routing to webapp is a special case. If the service contains "$HOST" the destination is webapp
      // Otherwise the destination service will be other cdap services.
      // Path lookup can be skipped for requests to webapp.
      RouteDestination destService = routerPathLookup.getRoutingService(service, path, httpRequest);
      if (destService == null || Strings.isNullOrEmpty(destService.getServiceName())
        || destService.getServiceName().equals(Constants.Router.DONT_ROUTE_SERVICE)) {
        return null;
      }

      CacheKey cacheKey = new CacheKey(destService, host, path);
      LOG.trace("Request was routed from {} to: {}", path, cacheKey.getRouteDestination());

      return discoverableCache.get(cacheKey);
    } catch (ExecutionException e) {
      return null;
    }
  }

  public void updateServiceMap(Map<Integer, String> serviceMap) {
    serviceMapRef.set(serviceMap);
  }

  private EndpointStrategy loadCache(CacheKey cacheKey) throws Exception {
    EndpointStrategy endpointStrategy;
    RouteDestination routeDestination = cacheKey.getRouteDestination();
    if (routeDestination.getServiceName().contains("$HOST")) {
      // Route URLs to host in the header.
      endpointStrategy = discoverService(cacheKey);

      if (endpointStrategy.pick() == null) {
        // Now try default, this matches any host / any port in the host header.
        endpointStrategy = discoverDefaultService(cacheKey);
      }
    } else {
      endpointStrategy = discover(routeDestination);
    }

    if (endpointStrategy.pick() == null) {
      String message = String.format("No discoverable endpoints found for service %s", cacheKey);
      LOG.debug(message);
      throw new Exception(message);
    }

    return endpointStrategy;
  }

  private EndpointStrategy discoverService(CacheKey key)
    throws UnsupportedEncodingException, ExecutionException {
    // First try with path routing
    String lookupService = genLookupName(key.getRouteDestination().getServiceName(),
                                         key.getHost(), key.getFirstPathPart());
    EndpointStrategy endpointStrategy = discover(new RouteDestination(lookupService));

    if (endpointStrategy.pick() == null) {
      // Try without path routing
      lookupService = genLookupName(key.getRouteDestination().getServiceName(), key.getHost());
      endpointStrategy = discover(new RouteDestination(lookupService));
    }

    return endpointStrategy;
  }

  private EndpointStrategy discoverDefaultService(CacheKey key)
    throws UnsupportedEncodingException, ExecutionException {
    // Try only path routing
    String lookupService = genLookupName(key.getRouteDestination().getServiceName(),
                                         DEFAULT_SERVICE_NAME, key.getFirstPathPart());
    return discover(new RouteDestination(lookupService));
  }

  private EndpointStrategy discover(RouteDestination routeDestination) throws ExecutionException {
    LOG.debug("Looking up service name {}", routeDestination);
    // If its a user service, then use DistributionEndpoint Strategy
    String serviceName = routeDestination.getServiceName();
    ServiceDiscovered serviceDiscovered = discoveryServiceClient.discover(serviceName);

    EndpointStrategy endpointStrategy = ServiceDiscoverable.isServiceDiscoverable(serviceName) ?
      new UserServiceEndpointStrategy(serviceDiscovered, routeStore, ServiceDiscoverable.getId(serviceName),
                                      fallbackStrategy, routeDestination.getVersion()) :
      new RandomEndpointStrategy(serviceDiscovered);
    if (endpointStrategy.pick(300L, TimeUnit.MILLISECONDS) == null) {
      LOG.debug("Discoverable endpoint {} not found", routeDestination);
    }
    return endpointStrategy;
  }

  private String genLookupName(String service, String host) throws UnsupportedEncodingException {
    String normalizedHost = Networks.normalizeWebappDiscoveryName(host);
    return service.replace("$HOST", normalizedHost);
  }

  private String genLookupName(String service, String host, String firstPathPart) throws UnsupportedEncodingException {
    String normalizedHost = Networks.normalizeWebappDiscoveryName(host + firstPathPart);
    return service.replace("$HOST", normalizedHost);
  }

  private static final class CacheKey {
    private final RouteDestination routeDestination;
    private final String host;
    private final String firstPathPart;
    private final int hashCode;

    private CacheKey(RouteDestination routeDestination, String host, String path) {
      this.routeDestination = routeDestination;
      this.host = host;
      int ind = path.indexOf('/', 1);
      this.firstPathPart = ind == -1 ? path : path.substring(0, ind);
      this.hashCode = Objects.hashCode(routeDestination, host, firstPathPart);
    }

    public RouteDestination getRouteDestination() {
      return routeDestination;
    }

    public String getHost() {
      return host;
    }

    public String getFirstPathPart() {
      return firstPathPart;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      CacheKey other = (CacheKey) o;
      return Objects.equal(routeDestination, other.routeDestination)
        && Objects.equal(host, other.host)
        && Objects.equal(firstPathPart, other.firstPathPart);
    }

    @Override
    public int hashCode() {
      return hashCode;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("routeDestination", routeDestination)
        .add("host", host)
        .add("firstPathPart", firstPathPart)
        .toString();
    }
  }
}
