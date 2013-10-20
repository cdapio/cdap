package com.continuuity.gateway.router;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.discovery.RandomEndpointStrategy;
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

          return new RandomEndpointStrategy(discoveryServiceClient.discover(serviceName));
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
        discoverable = discoverableCache.get(Constants.Service.GATEWAY).pick();
      } else {
        // Route other URLs to host in the header.
        String lookupService = genLookupName(service, headerInfo.getHost());
        discoverable = discoverableCache.get(lookupService).pick();

        if (discoverable == null) {
          // Another app may have replaced as the server to serve $HOST
          LOG.debug("Refreshing cache for service {}", lookupService);
          discoverableCache.refresh(lookupService);

          // Now try default host, this matches any host in the host header.
          lookupService = genLookupName(service, getDefaultHost(headerInfo.getHost()));
          discoverable = discoverableCache.get(lookupService).pick();
          if (discoverable == null) {
            LOG.debug("Refreshing cache for service {}", lookupService);
            discoverableCache.refresh(lookupService);
            LOG.warn("No discoverable endpoints found for service {}", service);
          }
        }
      }
    } else {
      discoverable = discoverableCache.get(service).pick();

      if (discoverable == null) {
        // Another app may have replaced as the server
        LOG.debug("Refreshing cache for service {}", service);
        discoverableCache.refresh(service);
      }
    }

    return discoverable;
  }

  public void updateServiceMap(Map<Integer, String> serviceMap) {
    serviceMapRef.set(serviceMap);
  }

  private String getDefaultHost(String host) {
    int portIndex = host.lastIndexOf(':');
    if (portIndex != -1) {
      return DEFAULT_SERVICE_NAME + host.substring(portIndex);
    } else {
      return DEFAULT_SERVICE_NAME;
    }
  }

  private String genLookupName(String service, String host) throws UnsupportedEncodingException {
    String normalizedHost = Networks.normalizeWebappHost(host);
    return service.replace("$HOST", normalizedHost);
  }
}
