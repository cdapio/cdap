package com.continuuity.gateway.router;

import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.weave.api.WeaveRunner;
import com.continuuity.weave.discovery.Discoverable;
import com.continuuity.weave.discovery.DiscoveryServiceClient;
import com.google.common.base.Charsets;
import com.google.common.base.Supplier;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Port -> service lookup.
 */
public class RouterServiceLookup implements ServiceLookup {
  private static final Logger LOG = LoggerFactory.getLogger(RouterServiceLookup.class);

  private final AtomicReference<Map<Integer, String>> serviceMapRef =
    new AtomicReference<Map<Integer, String>>(ImmutableMap.<Integer, String>of());

  private final Iterable<WeaveRunner.LiveInfo> liveApps;

  private final LoadingCache<String, EndpointStrategy> discoverableCache;

  @Inject
  public RouterServiceLookup(Iterable<WeaveRunner.LiveInfo> liveApps,
                             final DiscoveryServiceClient discoveryServiceClient) {
    this.liveApps = liveApps;

    this.discoverableCache = CacheBuilder.newBuilder()
      .expireAfterAccess(1, TimeUnit.HOURS)
      .build(new CacheLoader<String, EndpointStrategy>() {
        @Override
        public EndpointStrategy load(String key) throws Exception {
          return new RandomEndpointStrategy(discoveryServiceClient.discover(key));
        }
      });
  }

  @Override
  public String getService(int port) {
    return serviceMapRef.get().get(port);
  }

  @Override
  public Map<Integer, String> getServiceMap() {
    return ImmutableMap.copyOf(serviceMapRef.get());
  }

  @Override
  public Discoverable getDiscoverable(int port, Supplier<String> hostHeaderSupplier) throws Exception {
    String service = serviceMapRef.get().get(port);
    if (service == null) {
      LOG.debug("No service found for port {}", port);
      return null;
    }

    String host;
    if (service.contains("$HOST")) {
      host = hostHeaderSupplier.get();
      if (host == null) {
        LOG.debug("Cannot find host header for service {} on port {}", service, port);
        return null;
      }
      host = normalizeHost(host);
      service = service.replace("$HOST", host);
    }

    Discoverable discoverable = discoverableCache.get(service).pick();
    if (discoverable == null) {
      // Another app may have replaced as the server to serve $HOST
      LOG.debug("Refreshing cache for service {}", service);
      discoverableCache.refresh(service);

      // Try again
      discoverable = discoverableCache.get(service).pick();
      if (discoverable == null) {
        LOG.warn("No discoverable endpoints found for service {}", service);
      }
    }

    return discoverable;
  }

  public void updateServiceMap(Map<Integer, String> serviceMap) {
    serviceMapRef.set(serviceMap);
  }

  /**
   * Removes ":80" from end of the host and URL encodes it.
   * @param host host that needs to be normalized.
   * @return the normalized host.
   */
  static String normalizeHost(String host) throws UnsupportedEncodingException {
    if (host.endsWith(":80")) {
      host = host.substring(0, host.length() - 3);
    }

    return URLEncoder.encode(host, Charsets.UTF_8.name());
  }
}
