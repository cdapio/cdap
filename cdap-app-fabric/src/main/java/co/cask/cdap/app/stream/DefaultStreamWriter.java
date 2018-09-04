/*
 * Copyright © 2015-2018 Cask Data, Inc.
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
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.http.DefaultHttpRequestConfig;
import co.cask.cdap.common.id.Id;
import co.cask.cdap.common.internal.remote.RemoteClient;
import co.cask.cdap.common.service.Retries;
import co.cask.cdap.common.service.RetryStrategy;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.data2.metadata.writer.LineageWriter;
import co.cask.cdap.data2.registry.UsageWriter;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.net.HttpHeaders;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * Default implementation of {@link StreamWriter}
 */
public class DefaultStreamWriter implements StreamWriter {
  private final ConcurrentMap<StreamId, Boolean> isStreamRegistered;
  private final UsageWriter usageWriter;

  /**
   * The namespace that this {@link StreamWriter} belongs to.
   */
  private final NamespaceId namespace;
  /**
   * The owners of this {@link StreamWriter}.
   */
  private final Iterable<? extends EntityId> owners;
  private final ProgramRunId run;
  private final LineageWriter lineageWriter;
  private final AuthenticationContext authenticationContext;
  private final boolean authorizationEnabled;
  private final RetryStrategy retryStrategy;
  private final RemoteClient remoteClient;

  @Inject
  public DefaultStreamWriter(@Assisted("run") Id.Run run,
                             @Assisted("owners") Iterable<? extends EntityId> owners,
                             @Assisted("retryStrategy") RetryStrategy retryStrategy,
                             UsageWriter usageWriter,
                             LineageWriter lineageWriter,
                             DiscoveryServiceClient discoveryServiceClient,
                             AuthenticationContext authenticationContext,
                             CConfiguration cConf) {
    this.run = run.toEntityId();
    this.namespace = run.getNamespace().toEntityId();
    this.owners = owners;
    this.lineageWriter = lineageWriter;
    this.isStreamRegistered = Maps.newConcurrentMap();
    this.usageWriter = usageWriter;
    this.authenticationContext = authenticationContext;
    this.authorizationEnabled = cConf.getBoolean(Constants.Security.Authorization.ENABLED);
    this.retryStrategy = retryStrategy;
    this.remoteClient = new RemoteClient(
      discoveryServiceClient, Constants.Service.STREAMS, new DefaultHttpRequestConfig(false),
      String.format("%s/namespaces/%s/streams/", Constants.Gateway.API_VERSION_3, namespace.getNamespace()));
  }

  private void writeToStream(StreamId stream, HttpRequest.Builder builder) throws IOException {
    if (authorizationEnabled) {
      builder.addHeader(Constants.Security.Headers.USER_ID, authenticationContext.getPrincipal().getName());
    }
    HttpResponse response = remoteClient.execute(builder.build());
    int responseCode = response.getResponseCode();
    if (responseCode == HttpResponseStatus.NOT_FOUND.code()) {
      throw new IOException(String.format("Stream %s not found", stream));
    }

    // Even though we might have UnauthorizedException (FORBIDDEN), we need to register the usage/lineage since
    // the worker intended to write to the stream
    registerStream(stream);

    if (responseCode < 200 || responseCode >= 300) {
      throw new IOException(String.format("Writing to Stream %s did not succeed. Stream Service ResponseCode : %d",
                                          stream, responseCode));
    }
  }

  private void write(final String stream, final ByteBuffer data, final Map<String, String> headers) throws IOException {
    Retries.callWithRetries(new Retries.Callable<Void, IOException>() {
      @Override
      public Void call() throws IOException {
        HttpRequest.Builder requestBuilder = remoteClient.requestBuilder(HttpMethod.POST, stream).withBody(data);
        for (Map.Entry<String, String> header : headers.entrySet()) {
          requestBuilder.addHeader(stream + "." + header.getKey(), header.getValue());
        }
        writeToStream(namespace.stream(stream), requestBuilder);
        return null;
      }
    }, retryStrategy);
  }

  @Override
  public void write(String stream, String data) throws IOException {
    write(stream, data, ImmutableMap.<String, String>of());
  }

  @Override
  public void write(String stream, String data, Map<String, String> headers) throws IOException {
    write(stream, Charsets.UTF_8.encode(data), headers);
  }

  @Override
  public void write(String stream, ByteBuffer data) throws IOException {
    write(stream, data, ImmutableMap.<String, String>of());
  }

  @Override
  public void write(String stream, StreamEventData data) throws IOException {
    write(stream, data.getBody(), data.getHeaders());
  }

  @Override
  public void writeFile(final String stream, final File file, final String contentType) throws IOException {
    Retries.callWithRetries(new Retries.Callable<Void, IOException>() {
      @Override
      public Void call() throws IOException {
        HttpRequest.Builder requestBuilder = remoteClient.requestBuilder(HttpMethod.POST, stream + "/batch")
          .withBody(file)
          .addHeader(HttpHeaders.CONTENT_TYPE, contentType);
        writeToStream(namespace.stream(stream), requestBuilder);
        return null;
      }
    }, retryStrategy);
  }

  @Override
  public StreamBatchWriter createBatchWriter(final String stream, String contentType) throws IOException {
    URL url = Retries.callWithRetries(new Retries.Callable<URL, IOException>() {
      @Override
      public URL call() throws IOException {
        return remoteClient.resolve(stream + "/batch");
      }
    }, retryStrategy);
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod(HttpMethod.POST.name());
    connection.setReadTimeout(15000);
    connection.setConnectTimeout(15000);
    connection.setRequestProperty(HttpHeaders.CONTENT_TYPE, contentType);
    if (authorizationEnabled) {
      connection.setRequestProperty(Constants.Security.Headers.USER_ID, authenticationContext.getPrincipal().getName());
    }
    connection.setDoOutput(true);
    connection.setChunkedStreamingMode(0);
    connection.connect();
    try {
      StreamId streamId = namespace.stream(stream);
      registerStream(streamId);
      return new DefaultStreamBatchWriter(connection, streamId);
    } catch (IOException e) {
      connection.disconnect();
      throw e;
    }
  }

  private void registerStream(StreamId stream) {
    // prone to being entered multiple times, but OK since usageRegistry.register is not an expensive operation
    if (!isStreamRegistered.containsKey(stream)) {
      usageWriter.registerAll(owners, stream);
      isStreamRegistered.put(stream, true);
    }

    // Lineage writer handles duplicate accesses internally
    lineageWriter.addAccess(run, stream, AccessType.WRITE);
  }
}
