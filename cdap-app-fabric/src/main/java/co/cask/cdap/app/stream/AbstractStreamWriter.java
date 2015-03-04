/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.app.stream;

import co.cask.cdap.api.data.stream.StreamBatchWriter;
import co.cask.cdap.api.data.stream.StreamWriter;
import co.cask.cdap.api.stream.StreamEventData;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.services.AbstractServiceDiscoverer;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.proto.Id;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.net.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.discovery.Discoverable;
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

/**
 * Abstract implementation of {@link StreamWriter}
 */
public abstract class AbstractStreamWriter extends AbstractServiceDiscoverer implements StreamWriter {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractServiceDiscoverer.class);

  protected String namespaceId;
  protected String applicationId;

  public AbstractStreamWriter(Program program) {
    this.namespaceId = program.getNamespaceId();
    this.applicationId = program.getApplicationId();
  }

  private URL getStreamURL(String stream) throws IOException {
    return getStreamURL(stream, false);
  }

  private URL getStreamURL(String stream, boolean batch) throws IOException {
    ServiceDiscovered discovered = getDiscoveryServiceClient().discover(Constants.Service.STREAMS);
    Discoverable discoverable = new RandomEndpointStrategy(discovered).pick(1, TimeUnit.SECONDS);
    if (discoverable == null) {
      throw new IOException("Stream Service Endpoint not found");
    } else {
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
    return null;
  }

  private void writeToStream(Id.Stream stream, HttpRequest request) throws IOException {
    HttpResponse response = HttpRequests.execute(request);
    int responseCode = response.getResponseCode();
    if (responseCode == HttpResponseStatus.NOT_FOUND.code()) {
      throw new IOException(String.format("Stream %s not found", stream));
    } else if (responseCode < 200 || responseCode >= 300) {
      throw new IOException(String.format("Writing to Stream %s did not succeed", stream));
    }
  }

  private void write(String stream, ByteBuffer data, Map<String, String> headers) throws IOException {
    URL streamURL = getStreamURL(stream);
    HttpRequest request = HttpRequest.post(streamURL).withBody(data).addHeaders(headers).build();
    writeToStream(Id.Stream.from(namespaceId, stream), request);
  }

  @Override
  public void write(String stream, String data) throws IOException {
    write(stream, data, ImmutableMap.<String, String>of());
  }

  @Override
  public void write(String stream, String data, Map<String, String> headers) throws IOException {
    URL streamURL = getStreamURL(stream);
    HttpRequest request = HttpRequest.post(streamURL).withBody(data).addHeaders(headers).build();
    writeToStream(Id.Stream.from(namespaceId, stream), request);
  }

  @Override
  public void write(String stream, ByteBuffer data) throws IOException {
    write(stream, data, Maps.<String, String>newHashMap());
  }

  @Override
  public void write(String stream, StreamEventData data) throws IOException {
    write(stream, data.getBody(), data.getHeaders());
  }

  @Override
  public void writeFile(String stream, File file, String contentType) throws IOException {
    URL url = getStreamURL(stream, true);
    HttpRequest request = HttpRequest.post(url).withBody(file).addHeader(HttpHeaders.CONTENT_TYPE, contentType).build();
    writeToStream(Id.Stream.from(namespaceId, stream), request);
  }

  @Override
  public StreamBatchWriter createBatchWriter(String stream, String contentType) throws IOException {
    URL url = getStreamURL(stream, true);
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod(HttpMethod.POST.name());
    connection.setReadTimeout(15000);
    connection.setConnectTimeout(15000);
    connection.setRequestProperty(HttpHeaders.CONTENT_TYPE, contentType);
    connection.setDoOutput(true);
    connection.setChunkedStreamingMode(0);
    connection.connect();
    return new DefaultStreamBatchWriter(connection, Id.Stream.from(namespaceId, stream));
  }
}
