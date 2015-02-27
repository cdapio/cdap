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
import co.cask.cdap.api.data.stream.StreamBatchWriter;
import co.cask.cdap.api.data.stream.StreamContext;
import co.cask.cdap.api.stream.StreamEventData;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.common.exception.StreamNotFoundException;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.collect.Maps;
import com.google.common.net.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * An abstract implementation of {@link ServiceDiscoverer}.
 * It provides definition for {@link ServiceDiscoverer#getServiceURL}  and expects the sub-classes to give definition
 * for {@link AbstractServiceDiscoverer#getDiscoveryServiceClient}.
 */
public abstract class AbstractServiceDiscoverer implements ServiceDiscoverer, StreamContext {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractServiceDiscoverer.class);

  protected String namespaceId;
  protected String applicationId;

  protected AbstractServiceDiscoverer() {
  }

  public AbstractServiceDiscoverer(Program program) {
    this.namespaceId = program.getNamespaceId();
    this.applicationId = program.getApplicationId();
  }

  @Override
  public URL getServiceURL(String applicationId, String serviceId) {
    String discoveryName = String.format("service.%s.%s.%s", namespaceId, applicationId, serviceId);
    ServiceDiscovered discovered = getDiscoveryServiceClient().discover(discoveryName);
    return createURL(new RandomEndpointStrategy(discovered).pick(1, TimeUnit.SECONDS), applicationId, serviceId);
  }

  @Override
  public URL getServiceURL(String serviceId) {
    return getServiceURL(applicationId, serviceId);
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

  private URL getStreamURL(String stream) throws Exception {
    return getStreamURL(stream, false);
  }

  private URL getStreamURL(String stream, boolean batch) throws Exception {
    ServiceDiscovered discovered = getDiscoveryServiceClient().discover(Constants.Service.STREAMS);
    Discoverable discoverable = new RandomEndpointStrategy(discovered).pick(1, TimeUnit.SECONDS);
    if (discoverable != null) {
      InetSocketAddress address = discoverable.getSocketAddress();
      String path = String.format("http://%s:%d%s/namespaces/%s/streams/%s", address.getHostName(), address.getPort(),
                                  Constants.Gateway.API_VERSION_3, namespaceId, stream);
      if (batch) {
        path = String.format("%s/batch", path);
      }
      try {
        return new URL(path);
      } catch (MalformedURLException e) {
        LOG.error("Got exception while creating StreamURL", e);
      }
    }
    throw new Exception("Stream Endpoint not found");
  }

  private void writeToStream(HttpRequest request) throws IOException, StreamNotFoundException {
    HttpResponse response = HttpRequests.execute(request);
    if (response.getResponseCode() == HttpResponseStatus.NOT_FOUND.code()) {
      throw new StreamNotFoundException(String.format("%s not found", request.getURL()));
    }
  }

  public void write(String stream, String data) throws Exception {
    write(stream, data, Maps.<String, String>newHashMap());
  }

  public void write(String stream, String data, Map<String, String> headers) throws Exception {
    URL streamURL = getStreamURL(stream);
    HttpRequest request = HttpRequest.post(streamURL).withBody(data).addHeaders(headers).build();
    writeToStream(request);
  }

  public void write(String stream, ByteBuffer data) throws Exception {
    write(stream, data, Maps.<String, String>newHashMap());
  }

  public void write(String stream, ByteBuffer data, Map<String, String> headers) throws Exception {
    URL streamURL = getStreamURL(stream);
    HttpRequest request = HttpRequest.post(streamURL).withBody(data).addHeaders(headers).build();
    writeToStream(request);
  }

  public void write(String stream, StreamEventData data) throws Exception {
    write(stream, data.getBody(), data.getHeaders());
  }

  public void writeInBatch(String stream, File file, String contentType) throws Exception {
    URL url = getStreamURL(stream, true);
    HttpRequest request = HttpRequest.post(url).withBody(file).addHeader(HttpHeaders.CONTENT_TYPE, contentType).build();
    writeToStream(request);
  }

  public StreamBatchWriter writeInBatch(String stream, String contentType) throws Exception {
    URL url = getStreamURL(stream, true);
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod(HttpMethod.POST.name());
    connection.setReadTimeout(15000);
    connection.setConnectTimeout(15000);
    connection.setRequestProperty(HttpHeaders.CONTENT_TYPE, contentType);
    connection.setDoOutput(true);
    connection.setChunkedStreamingMode(0);
    connection.connect();
    return new DefaultStreamBatchWriter(connection, stream);
  }
}
