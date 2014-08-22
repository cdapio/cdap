/*
 * Copyright 2014 Cask, Inc.
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
package co.cask.cdap.data.stream.service;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.common.metrics.MetricsCollector;
import co.cask.cdap.common.metrics.MetricsScope;
import co.cask.cdap.data.stream.StreamCoordinator;
import co.cask.cdap.data.stream.StreamFileWriterFactory;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.AuthenticatedHttpHandler;
import co.cask.cdap.proto.StreamProperties;
import co.cask.http.HandlerContext;
import co.cask.http.HttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.base.CharMatcher;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closeables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * The {@link HttpHandler} for handling REST call to stream endpoints.
 *
 * TODO: Currently stream "dataset" is implementing old dataset API, hence not supporting multi-tenancy.
 */
@Path(Constants.Gateway.GATEWAY_VERSION + "/streams")
public final class StreamHandler extends AuthenticatedHttpHandler {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(StreamConfig.class, new StreamConfigAdapter())
    .create();

  private final CConfiguration cConf;
  private final StreamAdmin streamAdmin;
  private final ConcurrentStreamWriter streamWriter;

  // TODO: Need to make the decision of whether this should be inside StreamAdmin or not.
  // Currently is here to align with the existing Reactor organization that dataset admin is not aware of MDS
  private final StreamMetaStore streamMetaStore;

  @Inject
  public StreamHandler(CConfiguration cConf, Authenticator authenticator,
                       StreamCoordinator streamCoordinator, StreamAdmin streamAdmin, StreamMetaStore streamMetaStore,
                       StreamFileWriterFactory writerFactory,
                       MetricsCollectionService metricsCollectionService) {
    super(authenticator);
    this.cConf = cConf;
    this.streamAdmin = streamAdmin;
    this.streamMetaStore = streamMetaStore;

    MetricsCollector collector = metricsCollectionService.getCollector(MetricsScope.REACTOR, getMetricsContext(), "0");
    this.streamWriter = new ConcurrentStreamWriter(streamCoordinator, streamAdmin, streamMetaStore, writerFactory,
                                                   cConf.getInt(Constants.Stream.WORKER_THREADS, 10), collector);
  }

  @Override
  public void destroy(HandlerContext context) {
    Closeables.closeQuietly(streamWriter);
  }

  @GET
  @Path("/{stream}/info")
  public void getInfo(HttpRequest request, HttpResponder responder,
                      @PathParam("stream") String stream) throws Exception {
    String accountID = getAuthenticatedAccountId(request);

    if (streamMetaStore.streamExists(accountID, stream)) {
      StreamConfig streamConfig = streamAdmin.getConfig(stream);
      StreamProperties streamProperties = new StreamProperties(streamConfig.getName(), streamConfig.getTTL());
      responder.sendJson(HttpResponseStatus.OK, streamProperties, StreamProperties.class, GSON);
    } else {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    }
  }

  @PUT
  @Path("/{stream}")
  public void create(HttpRequest request, HttpResponder responder,
                     @PathParam("stream") String stream) throws Exception {

    String accountID = getAuthenticatedAccountId(request);

    // Verify stream name
    if (!isValidName(stream)) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST,
                           "Stream name can only contains alphanumeric, '-' and '_' characters only.");
      return;
    }

    // TODO: Modify the REST API to support custom configurations.
    streamAdmin.create(stream);
    streamMetaStore.addStream(accountID, stream);

    // TODO: For create successful, 201 Created should be returned instead of 200.
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/{stream}")
  public void enqueue(HttpRequest request, HttpResponder responder,
                      @PathParam("stream") String stream) throws Exception {

    String accountId = getAuthenticatedAccountId(request);

    try {
      if (streamWriter.enqueue(accountId, stream, getHeaders(request, stream), request.getContent().toByteBuffer())) {
        responder.sendStatus(HttpResponseStatus.OK);
      } else {
        responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
      }
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, "Stream does not exists");
    }
  }

  @POST
  @Path("/{stream}/truncate")
  public void truncate(HttpRequest request, HttpResponder responder,
                       @PathParam("stream") String stream) throws Exception {
    String accountId = getAuthenticatedAccountId(request);

    if (!streamMetaStore.streamExists(accountId, stream)) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, "Stream does not exists");
      return;
    }

    try {
      streamAdmin.truncate(stream);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (IOException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, "Stream does not exists");
    }
  }

  @PUT
  @Path("/{stream}/config")
  public void setConfig(HttpRequest request, HttpResponder responder,
                        @PathParam("stream") String stream) throws Exception {

    String accountId = getAuthenticatedAccountId(request);

    if (!streamMetaStore.streamExists(accountId, stream)) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, "Stream does not exists");
      return;
    }

    try {
      StreamConfig config = streamAdmin.getConfig(stream);
      try {
        config = getConfigUpdate(request, config);
        if (config.getTTL() < 0) {
          responder.sendString(HttpResponseStatus.BAD_REQUEST, "TTL value should be positive");
          return;
        }
      } catch (Throwable t) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid stream configuration");
        return;
      }

      streamAdmin.updateConfig(config);
      responder.sendStatus(HttpResponseStatus.OK);

    } catch (IOException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, "Stream does not exists");
    }
  }

  private String getMetricsContext() {
    return Constants.Gateway.METRICS_CONTEXT + "." + cConf.getInt(Constants.Stream.CONTAINER_INSTANCE_ID, 0);
  }


  private StreamConfig getConfigUpdate(HttpRequest request, StreamConfig config) {
    JsonObject json = GSON.fromJson(request.getContent().toString(Charsets.UTF_8), JsonObject.class);

    // Only pickup changes in TTL
    if (json.has("ttl")) {
      JsonElement ttl = json.get("ttl");
      if (ttl.isJsonPrimitive()) {
        // TTL in the REST API is in seconds. Convert it to ms for the config.
        return new StreamConfig(config.getName(), config.getPartitionDuration(), config.getIndexInterval(),
                                TimeUnit.SECONDS.toMillis(ttl.getAsLong()), config.getLocation());
      }
    }
    return config;
  }

  private boolean isValidName(String streamName) {
    // TODO: This is copied from StreamVerification in app-fabric as this handler is in data-fabric module.
    return CharMatcher.inRange('A', 'Z')
      .or(CharMatcher.inRange('a', 'z'))
      .or(CharMatcher.is('-'))
      .or(CharMatcher.is('_'))
      .or(CharMatcher.inRange('0', '9')).matchesAllOf(streamName);
  }

  private Map<String, String> getHeaders(HttpRequest request, String stream) {
    // build a new event from the request, start with the headers
    ImmutableMap.Builder<String, String> headers = ImmutableMap.builder();
    // and transfer all other headers that are to be preserved
    String prefix = stream + ".";
    for (Map.Entry<String, String> header : request.getHeaders()) {
      if (header.getKey().startsWith(prefix)) {
        headers.put(header.getKey().substring(prefix.length()), header.getValue());
      }
    }
    return headers.build();
  }

  /**
   *  Adapter class for {@link co.cask.cdap.data2.transaction.stream.StreamConfig}
   */
  private static final class StreamConfigAdapter implements JsonSerializer<StreamConfig> {
    @Override
    public JsonElement serialize(StreamConfig src, Type typeOfSrc, JsonSerializationContext context) {
      JsonObject json = new JsonObject();
      json.addProperty("partitionDuration", src.getPartitionDuration());
      json.addProperty("indexInterval", src.getIndexInterval());
      json.addProperty("ttl", TimeUnit.MILLISECONDS.toSeconds(src.getTTL()));
      return json;
    }
  }
}
