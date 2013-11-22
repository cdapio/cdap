package com.continuuity.gateway.router;

import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.discovery.TimeLimitEndpointStrategy;
import com.continuuity.common.utils.Networks;
import com.continuuity.weave.discovery.Discoverable;
import com.continuuity.weave.discovery.DiscoveryServiceClient;
import com.google.common.base.Objects;
import com.google.common.base.Supplier;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
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

  @Inject
  public RouterServiceLookup(DiscoveryServiceClient discoveryServiceClient) {
    this.discoveryServiceClient = discoveryServiceClient;

    this.discoverableCache = CacheBuilder.newBuilder()
      .expireAfterAccess(1, TimeUnit.HOURS)
      .build(new CacheLoader<CacheKey, EndpointStrategy>() {
        @Override
        public EndpointStrategy load(CacheKey key) throws Exception {
          return loadCache(key);
        }
      });
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
     * @param hostHeaderSupplier supplies the header information for the lookup.
     * @return discoverable based on port and host header.
     */
  public Discoverable getDiscoverable(int port, Supplier<HeaderDecoder.HeaderInfo> hostHeaderSupplier)
    throws Exception {
    final String service = serviceMapRef.get().get(port);
    if (service == null) {
      LOG.debug("No service found for port {}", port);
      return null;
    }

    HeaderDecoder.HeaderInfo headerInfo = hostHeaderSupplier.get();
    if (headerInfo == null) {
      LOG.debug("Cannot find host header for service {} on port {}", service, port);
      return null;
    }

    try {
      CacheKey cacheKey = new CacheKey(service, headerInfo);
      Discoverable discoverable = discoverableCache.get(cacheKey).pick();
      if (discoverable == null) {
        // Looks like the service is no longer running.
        LOG.debug("Invalidating cache for service {} on port {}", service, port);
        discoverableCache.invalidate(cacheKey);
      }
      return discoverable;
    } catch (ExecutionException e) {
      return null;
    }
  }

  public void updateServiceMap(Map<Integer, String> serviceMap) {
    serviceMapRef.set(serviceMap);
  }

  private EndpointStrategy loadCache(CacheKey cacheKey) throws Exception {
    EndpointStrategy endpointStrategy;
    String service = cacheKey.getService();
    if (service.contains("$HOST")) {
      // Route URLs to host in the header.
      endpointStrategy = discoverService(cacheKey);

      if (endpointStrategy.pick() == null) {
        // Now try default, this matches any host / any port in the host header.
        endpointStrategy = discoverDefaultService(cacheKey);
      }
    } else {
      endpointStrategy = discover(service);
    }

    if (endpointStrategy.pick() == null) {
      String message = String.format("No discoverable endpoints found for service %s", cacheKey);
      LOG.error(message);
      throw new Exception(message);
    }

    return endpointStrategy;
  }

  private EndpointStrategy discoverService(CacheKey key)
    throws UnsupportedEncodingException, ExecutionException {
    // First try with path routing
    String lookupService = genLookupName(key.getService(), key.getHostHeaader(), key.getFirstPathPart());
    EndpointStrategy endpointStrategy = discover(lookupService);

    if (endpointStrategy.pick() == null) {
      // Try without path routing
      lookupService = genLookupName(key.getService(), key.getHostHeaader());
      endpointStrategy = discover(lookupService);
    }

    return endpointStrategy;
  }

  private EndpointStrategy discoverDefaultService(CacheKey key)
    throws UnsupportedEncodingException, ExecutionException {
    // Try only path routing
    String lookupService = genLookupName(key.getService(), DEFAULT_SERVICE_NAME, key.getFirstPathPart());
    return discover(lookupService);
  }

  private EndpointStrategy discover(String discoverName) throws ExecutionException {
    LOG.debug("Looking up service name {}", discoverName);

    EndpointStrategy endpointStrategy = new RandomEndpointStrategy(discoveryServiceClient.discover(discoverName));
    if (new TimeLimitEndpointStrategy(endpointStrategy, 300L, TimeUnit.MILLISECONDS).pick() == null) {
      LOG.debug("Discoverable endpoint {} not found", discoverName);
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
    private final String service;
    private final String hostHeaader;
    private final String firstPathPart;

    private CacheKey(String service, HeaderDecoder.HeaderInfo headerInfo) {
      this.service = service;
      this.hostHeaader = headerInfo.getHost();

      String path = headerInfo.getPath().replaceAll("/+", "/");
      int ind = path.indexOf('/', 1);
      this.firstPathPart = ind == -1 ? path : path.substring(0, ind);
    }

    public String getService() {
      return service;
    }

    public String getHostHeaader() {
      return hostHeaader;
    }

    public String getFirstPathPart() {
      return firstPathPart;
    }

    @SuppressWarnings("RedundantIfStatement")
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      CacheKey cacheKey = (CacheKey) o;

      if (firstPathPart != null ? !firstPathPart.equals(cacheKey.firstPathPart) : cacheKey.firstPathPart != null) {
        return false;
      }
      if (hostHeaader != null ? !hostHeaader.equals(cacheKey.hostHeaader) : cacheKey.hostHeaader != null) {
        return false;
      }
      if (service != null ? !service.equals(cacheKey.service) : cacheKey.service != null) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      int result = service != null ? service.hashCode() : 0;
      result = 31 * result + (hostHeaader != null ? hostHeaader.hashCode() : 0);
      result = 31 * result + (firstPathPart != null ? firstPathPart.hashCode() : 0);
      return result;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("service", service)
        .add("hostHeaader", hostHeaader)
        .add("firstPathPart", firstPathPart)
        .toString();
    }
  }
}
