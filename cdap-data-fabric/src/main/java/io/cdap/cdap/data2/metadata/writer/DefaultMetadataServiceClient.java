/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.data2.metadata.writer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.metadata.elastic.ScopedNameOfKindTypeAdapter;
import io.cdap.cdap.metadata.elastic.ScopedNameTypeAdapter;
import io.cdap.cdap.proto.codec.NamespacedEntityIdCodec;
import io.cdap.cdap.proto.id.NamespacedEntityId;
import io.cdap.cdap.spi.metadata.Metadata;
import io.cdap.cdap.spi.metadata.MetadataCodec;
import io.cdap.cdap.spi.metadata.MetadataMutation;
import io.cdap.cdap.spi.metadata.MetadataMutationCodec;
import io.cdap.cdap.spi.metadata.ScopedName;
import io.cdap.cdap.spi.metadata.ScopedNameOfKind;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import javax.inject.Inject;

/**
 * Metadata service client that allows CDAP Master to make Metadata updates via HTTP.
 */
public class DefaultMetadataServiceClient implements MetadataServiceClient {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultMetadataServiceClient.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(NamespacedEntityId.class, new NamespacedEntityIdCodec())
    .registerTypeAdapter(Metadata.class, new MetadataCodec())
    .registerTypeAdapter(ScopedName.class, new ScopedNameTypeAdapter())
    .registerTypeAdapter(ScopedNameOfKind.class, new ScopedNameOfKindTypeAdapter())
    .registerTypeAdapter(MetadataMutation.class, new MetadataMutationCodec())
    .create();
  private final RemoteClient remoteClient;

  @Inject
  public DefaultMetadataServiceClient(final DiscoveryServiceClient discoveryClient) {
    this.remoteClient = new RemoteClient(discoveryClient, Constants.Service.METADATA_SERVICE,
                                         new DefaultHttpRequestConfig(false),
                                         Constants.Gateway.API_VERSION_3);
  }

  @Override
  public void create(MetadataMutation.Create createMutation) {
    HttpRequest request = remoteClient.requestBuilder(HttpMethod.POST, "metadata-internals/create")
      .withBody(GSON.toJson(createMutation)).build();
    HttpResponse response = execute(request);

    if (HttpResponseStatus.OK.code() != response.getResponseCode()) {
      LOG.trace("Failed to create metadata for entity %s: %s", createMutation.getEntity(), response);
    }
  }

  @Override
  public void drop(MetadataMutation.Drop dropMutation) {
    HttpRequest request = remoteClient.requestBuilder(HttpMethod.DELETE, "metadata-internals/drop")
      .withBody(GSON.toJson(dropMutation)).build();
    HttpResponse response = execute(request);

    if (HttpResponseStatus.OK.code() != response.getResponseCode()) {
      LOG.trace("Failed to drop metadata for entity %s: %s", dropMutation.getEntity(), response);
    }
  }

  @Override
  public void update(MetadataMutation.Update updateMutation) {
    HttpRequest request = remoteClient.requestBuilder(HttpMethod.POST, "metadata-internals/update")
      .withBody(GSON.toJson(updateMutation)).build();
    HttpResponse response = execute(request);

    if (HttpResponseStatus.OK.code() != response.getResponseCode()) {
      LOG.trace("Failed to update metadata for entity %s: %s", updateMutation.getEntity(), response);
    }
  }

  @Override
  public void remove(MetadataMutation.Remove removeMutation) {
    HttpRequest request = remoteClient.requestBuilder(HttpMethod.DELETE, "metadata-internals/remove")
      .withBody(GSON.toJson(removeMutation)).build();
    HttpResponse response = execute(request);

    if (HttpResponseStatus.OK.code() != response.getResponseCode()) {
      LOG.trace("Failed to remove metadata for entity %s: %s", removeMutation.getEntity(), response);
    }
  }

  @Override
  public void batch(List<MetadataMutation> mutations) {
    HttpRequest request = remoteClient.requestBuilder(HttpMethod.POST, "metadata-internals/batch")
      .withBody(GSON.toJson(mutations)).build();
    HttpResponse response = execute(request);
    if (HttpResponseStatus.OK.code() != response.getResponseCode()) {
      LOG.trace("Failed to apply metadata mutations for mutations %s: %s", mutations, response);
    }
  }

  protected HttpResponse execute(HttpRequest request) {
    try {
      LOG.trace("Making metadata mutation request {}", request);
      HttpResponse response = remoteClient.execute(request);
      LOG.trace("Received response {} for request {}", response, request);
      return response;
    } catch (Exception e) {
      throw new RuntimeException("Failed to execute metadata mutation, with request: " + request, e);
    }
  }
}
