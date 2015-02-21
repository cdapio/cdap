/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.app.services;

import co.cask.cdap.api.ServiceDiscoverer;
import co.cask.cdap.api.data.stream.StreamContext;
import co.cask.cdap.api.data.stream.StreamWriteStatus;
import co.cask.cdap.api.stream.StreamEventData;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.EndpointStrategy;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Joiner;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * An abstract implementation of {@link ServiceDiscoverer}.
 * It provides definition for {@link ServiceDiscoverer#getServiceURL}  and expects the sub-classes to give definition
 * for {@link AbstractServiceDiscoverer#getDiscoveryServiceClient}.
 */
public abstract class AbstractServiceDiscoverer implements ServiceDiscoverer, StreamContext {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractServiceDiscoverer.class);

  private LoadingCache<String, EndpointStrategy> loadingCache;
  protected String namespaceId;
  protected String applicationId;

  protected AbstractServiceDiscoverer() {
  }

  public AbstractServiceDiscoverer(Program program) {
    this.namespaceId = program.getNamespaceId();
    this.applicationId = program.getApplicationId();
    this.loadingCache = CacheBuilder.newBuilder().expireAfterAccess(1, TimeUnit.HOURS).build(
      new CacheLoader<String, EndpointStrategy>() {
      @Override
      public EndpointStrategy load(String discoverableName) throws Exception {
        return getEndpointStrategy(discoverableName);
      }
    });
  }

  @Override
  public URL getServiceURL(String applicationId, String serviceId) {
    String discoveryName = String.format("service.%s.%s.%s", namespaceId, applicationId, serviceId);
    try {
      return createURL(loadingCache.get(discoveryName).pick(1, TimeUnit.SECONDS), applicationId, serviceId);
    } catch (ExecutionException e) {
      return null;
    }
  }

  @Override
  public URL getServiceURL(String serviceId) {
    return getServiceURL(applicationId, serviceId);
  }

  private EndpointStrategy getEndpointStrategy(String discoveryName) {
    ServiceDiscovered discovered = getDiscoveryServiceClient().discover(discoveryName);
    return new RandomEndpointStrategy(discovered);
  }

  /**
   * @return the {@link DiscoveryServiceClient} for Service Discovery
   */
  protected abstract DiscoveryServiceClient getDiscoveryServiceClient();

  @Nullable
  private URL createURL(@Nullable Discoverable discoverable, String applicationId, String serviceId) {
    if (discoverable == null) {
      return null;
    }
    InetSocketAddress address = discoverable.getSocketAddress();
    String path = String.format("http://%s:%d%s/namespaces/%s/apps/%s/services/%s/methods/",
                                address.getHostName(), address.getPort(),
                                Constants.Gateway.API_VERSION_3, namespaceId, applicationId, serviceId);
    try {
      return new URL(path);
    } catch (MalformedURLException e) {
      LOG.error("Got exception while creating serviceURL", e);
      return null;
    }
  }

  private StreamWriteStatus write(String stream, ByteBuffer data, Map<String, String> headers, boolean batch) {
    Discoverable discoverable = null;
    try {
      discoverable = loadingCache.get(Constants.Service.STREAMS).pick(1, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      // no-op
    }

    if (discoverable != null) {
      InetSocketAddress address = discoverable.getSocketAddress();
      String path = String.format("http://%s:%d%s/namespaces/%s/streams/%s", address.getHostName(), address.getPort(),
                                  Constants.Gateway.API_VERSION_3, namespaceId, stream);
      if (batch) {
        path = String.format("%s/batch", path);
      }

      try {
        HttpRequest request = HttpRequest.post(new URL(path)).addHeaders(headers).withBody(data).build();
        HttpResponse status = HttpRequests.execute(request);
        return status.getResponseCode() == HttpResponseStatus.OK.code() ?
          StreamWriteStatus.SUCCESS : StreamWriteStatus.FAILURE;
      } catch (Exception e) {
        LOG.debug("Unable to write to Stream {}:{}", namespaceId, stream, e);
      }
    }
    return StreamWriteStatus.NOT_FOUND;
  }

  @Override
  public StreamWriteStatus writeToStream(String stream, StreamEventData data) {
    return write(stream, data.getBody(), data.getHeaders(), false);
  }

  @Override
  public StreamWriteStatus writeToStream(String stream, byte[] data) {
    return writeToStream(stream, data, ImmutableMap.<String, String>of());
  }

  @Override
  public StreamWriteStatus writeToStream(String stream, byte[] data, Map<String, String> headers) {
    return writeToStream(stream, new StreamEventData(headers, ByteBuffer.wrap(data)));
  }

  @Override
  public StreamWriteStatus writeToStream(String stream, List<byte[]> data) {
    return writeToStream(stream, data, ImmutableMap.<String, String>of());
  }

  @Override
  public StreamWriteStatus writeToStream(String stream, List<byte[]> data, Map<String, String> headers) {
    byte[] combinedData = Joiner.on('\n').join(data).getBytes();
    return write(stream, ByteBuffer.wrap(combinedData), headers, true);
  }
}
