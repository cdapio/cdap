/*
 * Copyright © 2014 Cask Data, Inc.
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
import co.cask.cdap.explore.client.ExploreFacade;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.AuthenticatedHttpHandler;
import co.cask.cdap.internal.io.Schema;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
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
import org.apache.twill.common.Threads;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
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
@Path(Constants.Gateway.API_VERSION_2 + "/streams")
public final class StreamHandler extends AuthenticatedHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(StreamHandler.class);

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(StreamProperties.class, new StreamPropertiesAdapter())
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();

  private final CConfiguration cConf;
  private final StreamAdmin streamAdmin;
  private final MetricsCollector metricsCollector;
  private final ConcurrentStreamWriter streamWriter;
  private final ExploreFacade exploreFacade;
  private final boolean exploreEnabled;
  private final StreamLeaderManager streamLeaderManager;

  // Executor for serving async enqueue requests
  private ExecutorService asyncExecutor;

  // TODO: Need to make the decision of whether this should be inside StreamAdmin or not.
  // Currently is here to align with the existing CDAP organization that dataset admin is not aware of MDS
  private final StreamMetaStore streamMetaStore;

  @Inject
  public StreamHandler(CConfiguration cConf, Authenticator authenticator,
                       StreamCoordinator streamCoordinator, StreamAdmin streamAdmin, StreamMetaStore streamMetaStore,
                       StreamFileWriterFactory writerFactory,
                       MetricsCollectionService metricsCollectionService,
                       ExploreFacade exploreFacade, StreamLeaderManager streamLeaderManager) {
    super(authenticator);
    this.cConf = cConf;
    this.streamAdmin = streamAdmin;
    this.streamMetaStore = streamMetaStore;
    this.exploreFacade = exploreFacade;
    this.exploreEnabled = cConf.getBoolean(Constants.Explore.EXPLORE_ENABLED);
    this.streamLeaderManager = streamLeaderManager;

    this.metricsCollector = metricsCollectionService.getCollector(MetricsScope.SYSTEM, getMetricsContext(), "0");
    this.streamWriter = new ConcurrentStreamWriter(streamCoordinator, streamAdmin, streamMetaStore, writerFactory,
                                                   cConf.getInt(Constants.Stream.WORKER_THREADS), metricsCollector);
  }

  @Override
  public void init(HandlerContext context) {
    super.init(context);
    int asyncWorkers = cConf.getInt(Constants.Stream.ASYNC_WORKER_THREADS);
    // The queue size config is size per worker, hence multiple by workers here
    int asyncQueueSize = cConf.getInt(Constants.Stream.ASYNC_QUEUE_SIZE) * asyncWorkers;

    // Creates a thread pool that will shrink inactive threads
    // Also, it limits how many tasks can get queue up to guard against out of memory if incoming requests are
    // coming too fast.
    // It uses the caller thread execution rejection policy so that it slows down request naturally by resorting
    // to sync enqueue (enqueue by caller thread is the same as sync enqueue)
    ThreadPoolExecutor executor = new ThreadPoolExecutor(asyncWorkers, asyncWorkers, 60, TimeUnit.SECONDS,
                                                         new ArrayBlockingQueue<Runnable>(asyncQueueSize),
                                                         Threads.createDaemonThreadFactory("async-exec-%d"),
                                                         createAsyncRejectedExecutionHandler());
    executor.allowCoreThreadTimeOut(true);
    asyncExecutor = executor;
  }

  @Override
  public void destroy(HandlerContext context) {
    Closeables.closeQuietly(streamWriter);
    asyncExecutor.shutdownNow();
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
    // Enable ad-hoc exploration of stream
    if (exploreEnabled) {
      try {
        exploreFacade.enableExploreStream(stream);
      } catch (Exception e) {
        // at this time we want to still allow using stream even if it cannot be used for exploration
        String msg = String.format("Cannot enable exploration of stream %s: %s", stream, e.getMessage());
        LOG.error(msg, e);
      }
    }

    streamLeaderManager.affectLeader(stream);

    // TODO: For create successful, 201 Created should be returned instead of 200.
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/{stream}")
  public void enqueue(HttpRequest request, HttpResponder responder,
                      @PathParam("stream") String stream) throws Exception {

    String accountId = getAuthenticatedAccountId(request);

    try {
      streamWriter.enqueue(accountId, stream, getHeaders(request, stream), request.getContent().toByteBuffer());
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, "Stream does not exists");
    } catch (IOException e) {
      LOG.error("Failed to write to stream {}", stream, e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  @POST
  @Path("/{stream}/async")
  public void asyncEnqueue(HttpRequest request, HttpResponder responder,
                           @PathParam("stream") final String stream) throws Exception {
    String accountId = getAuthenticatedAccountId(request);
    // No need to copy the content buffer as we always uses a ChannelBufferFactory that won't reuse buffer.
    // See StreamHttpService
    streamWriter.asyncEnqueue(accountId, stream,
                              getHeaders(request, stream), request.getContent().toByteBuffer(), asyncExecutor);
    responder.sendStatus(HttpResponseStatus.ACCEPTED);
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
                                TimeUnit.SECONDS.toMillis(ttl.getAsLong()), config.getLocation(), config.getFormat());
      }
    }
    return config;
  }

  private RejectedExecutionHandler createAsyncRejectedExecutionHandler() {
    return new RejectedExecutionHandler() {
      @Override
      public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        if (!executor.isShutdown()) {
          metricsCollector.increment("collect.async.reject", 1);
          r.run();
        }
      }
    };
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
   *  Adapter class for {@link co.cask.cdap.proto.StreamProperties}
   */
  private static final class StreamPropertiesAdapter implements JsonSerializer<StreamProperties> {
    @Override
    public JsonElement serialize(StreamProperties src, Type typeOfSrc, JsonSerializationContext context) {
      JsonObject json = new JsonObject();
      json.addProperty("name", src.getName());
      json.addProperty("ttl", TimeUnit.MILLISECONDS.toSeconds(src.getTTL()));
      return json;
    }
  }
}
