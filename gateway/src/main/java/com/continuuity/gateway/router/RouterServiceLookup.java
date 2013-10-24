package com.continuuity.gateway.router;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.discovery.TimeLimitEndpointStrategy;
import com.continuuity.common.utils.Networks;
import com.continuuity.weave.discovery.Discoverable;
import com.continuuity.weave.discovery.DiscoveryServiceClient;
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
  private static final String GATEWAY_URL_PREFIX = Constants.Gateway.GATEWAY_VERSION + "/";
  private static final String DEFAULT_SERVICE_NAME = "default";

  private final AtomicReference<Map<Integer, String>> serviceMapRef =
    new AtomicReference<Map<Integer, String>>(ImmutableMap.<Integer, String>of());

  private final LoadingCache<String, EndpointStrategy> discoverableCache;

  @Inject
  public RouterServiceLookup(final DiscoveryServiceClient discoveryServiceClient) {

    this.discoverableCache = CacheBuilder.newBuilder()
      .expireAfterAccess(1, TimeUnit.HOURS)
      .build(new CacheLoader<String, EndpointStrategy>() {
        @Override
        public EndpointStrategy load(String serviceName) throws Exception {
          LOG.debug("Looking up service name {}", serviceName);

          return new TimeLimitEndpointStrategy(
            new RandomEndpointStrategy(discoveryServiceClient.discover(serviceName)),
            1000L, TimeUnit.MILLISECONDS);
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

    Discoverable discoverable;
    if (service.contains("$HOST")) {
      HeaderDecoder.HeaderInfo headerInfo = hostHeaderSupplier.get();
      if (headerInfo == null) {
        LOG.debug("Cannot find host header for service {} on port {}", service, port);
        return null;
      }

      // Route gateway URLs to gateway.
      if (headerInfo.getPath().startsWith(GATEWAY_URL_PREFIX)) {
        discoverable = discover(Constants.Service.GATEWAY);
      } else {
        // Route other URLs to host in the header.
        discoverable = discoverService(service, headerInfo);

        if (discoverable == null) {
          // Now try default, this matches any host / any port in the host header.
          discoverable = discoverDefaultService(service, headerInfo);
        }

        if (discoverable == null) {
          LOG.error("No discoverable endpoints found for service {} {}", service, headerInfo);
        }
      }
    } else {
      discoverable = discover(service);

      if (discoverable == null) {
        LOG.error("No discoverable endpoints found for service {}", service);
      }
    }

    return discoverable;
  }

  public void updateServiceMap(Map<Integer, String> serviceMap) {
    serviceMapRef.set(serviceMap);
  }

  private Discoverable discoverService(String service, HeaderDecoder.HeaderInfo headerInfo)
    throws UnsupportedEncodingException, ExecutionException {
    // First try with path routing
    String lookupService = genLookupName(service, headerInfo.getHost(), headerInfo.getPath());
    Discoverable discoverable = discover(lookupService);

    if (discoverable == null) {
      // Try without path routing
      lookupService = genLookupName(service, headerInfo.getHost());
      discoverable = discover(lookupService);
    }

    return discoverable;
  }

  private Discoverable discoverDefaultService(String service, HeaderDecoder.HeaderInfo headerInfo)
    throws UnsupportedEncodingException, ExecutionException {
    // Try only path routing
    String lookupService = genLookupName(service, DEFAULT_SERVICE_NAME, headerInfo.getPath());
    return discover(lookupService);
  }

  private Discoverable discover(String discoverName) throws ExecutionException {
    Discoverable discoverable = discoverableCache.get(discoverName).pick();

    if (discoverable == null) {
      LOG.debug("Discoverable endpoint {} not found", discoverName);
    }
    return discoverable;
  }

  private String genLookupName(String service, String host) throws UnsupportedEncodingException {
    String normalizedHost = Networks.normalizeWebappDiscoveryName(host);
    return service.replace("$HOST", normalizedHost);
  }

  private String genLookupName(String service, String host, String requestPath) throws UnsupportedEncodingException {
    String pathPart = requestPath;
    int ind = pathPart.indexOf('/', 1);
    if (ind != -1) {
      pathPart = pathPart.substring(0, ind);
    }
    String normalizedHost = Networks.normalizeWebappDiscoveryName(host + pathPart);
    return service.replace("$HOST", normalizedHost);
  }
}
