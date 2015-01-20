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
package co.cask.cdap.data.stream.service;

import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.format.RecordFormat;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.common.metrics.MetricsCollector;
import co.cask.cdap.data.format.RecordFormats;
import co.cask.cdap.data.stream.StreamCoordinatorClient;
import co.cask.cdap.data.stream.StreamFileWriterFactory;
import co.cask.cdap.data.stream.service.upload.BufferedContentWriterFactory;
import co.cask.cdap.data.stream.service.upload.ContentWriterFactory;
import co.cask.cdap.data.stream.service.upload.FileContentWriterFactory;
import co.cask.cdap.data.stream.service.upload.StreamBodyConsumerFactory;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.explore.client.ExploreFacade;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.AuthenticatedHttpHandler;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.cdap.proto.StreamProperties;
import co.cask.http.BodyConsumer;
import co.cask.http.HandlerContext;
import co.cask.http.HttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.base.CharMatcher;
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
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
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
  private final long batchBufferThreshold;
  private final StreamBodyConsumerFactory streamBodyConsumerFactory;

  // Executor for serving async enqueue requests
  private ExecutorService asyncExecutor;

  // TODO: Need to make the decision of whether this should be inside StreamAdmin or not.
  // Currently is here to align with the existing CDAP organization that dataset admin is not aware of MDS
  private final StreamMetaStore streamMetaStore;

  private final StreamWriterSizeCollector sizeCollector;

  @Inject
  public StreamHandler(CConfiguration cConf, Authenticator authenticator,
                       StreamCoordinatorClient streamCoordinatorClient, StreamAdmin streamAdmin,
                       StreamMetaStore streamMetaStore, StreamFileWriterFactory writerFactory,
                       MetricsCollectionService metricsCollectionService,
                       ExploreFacade exploreFacade, StreamWriterSizeCollector sizeCollector) {
    super(authenticator);
    this.cConf = cConf;
    this.streamAdmin = streamAdmin;
    this.streamMetaStore = streamMetaStore;
    this.exploreFacade = exploreFacade;
    this.sizeCollector = sizeCollector;
    this.exploreEnabled = cConf.getBoolean(Constants.Explore.EXPLORE_ENABLED);
    this.batchBufferThreshold = cConf.getLong(Constants.Stream.BATCH_BUFFER_THRESHOLD);
    this.streamBodyConsumerFactory = new StreamBodyConsumerFactory();
    this.metricsCollector = metricsCollectionService.getCollector(getMetricsContext());
    StreamMetricsCollectorFactory metricsCollectorFactory = createStreamMetricsCollectorFactory();
    this.streamWriter = new ConcurrentStreamWriter(streamCoordinatorClient, streamAdmin, streamMetaStore, writerFactory,
                                                   cConf.getInt(Constants.Stream.WORKER_THREADS),
                                                   metricsCollectorFactory);
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
      StreamProperties streamProperties =
        new StreamProperties(streamConfig.getName(), streamConfig.getTTL(), streamConfig.getFormat());
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
                           @PathParam("stream") String stream) throws Exception {
    String accountId = getAuthenticatedAccountId(request);
    // No need to copy the content buffer as we always uses a ChannelBufferFactory that won't reuse buffer.
    // See StreamHttpService
    streamWriter.asyncEnqueue(accountId, stream, getHeaders(request, stream),
                              request.getContent().toByteBuffer(), asyncExecutor);
    responder.sendStatus(HttpResponseStatus.ACCEPTED);
  }

  @POST
  @Path("/{stream}/batch")
  public BodyConsumer batch(HttpRequest request, HttpResponder responder,
                            @PathParam("stream") String stream) throws Exception {
    String accountId = getAuthenticatedAccountId(request);

    if (!streamMetaStore.streamExists(accountId, stream)) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, "Stream does not exists");
      return null;
    }

    try {
      return streamBodyConsumerFactory.create(request, createContentWriterFactory(accountId, stream, request));
    } catch (UnsupportedOperationException e) {
      responder.sendString(HttpResponseStatus.NOT_ACCEPTABLE, e.getMessage());
      return null;
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
      responder.sendString(HttpResponseStatus.NOT_FOUND, "Stream does not exist.");
      return;
    }

    StreamConfig currConfig;
    try {
      currConfig = streamAdmin.getConfig(stream);
    } catch (IOException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, "Stream " + stream + " does not exist.");
      return;
    }

    StreamConfig requestedConfig = getAndValidateConfig(currConfig, request, responder);
    // null is returned if the requested config is invalid. An appropriate response will have already been written
    // to the responder so we just need to return.
    if (requestedConfig == null) {
      return;
    }

    streamAdmin.updateConfig(requestedConfig);
    // if the schema has changed, we need to recreate the hive table. Changes in format and settings don't require
    // a hive change, as they are just properties used by the stream storage handler.
    Schema currSchema = currConfig.getFormat().getSchema();
    Schema newSchema = requestedConfig.getFormat().getSchema();
    if (exploreEnabled && !currSchema.equals(newSchema)) {
      exploreFacade.disableExploreStream(stream);
      exploreFacade.enableExploreStream(stream);
    }

    responder.sendStatus(HttpResponseStatus.OK);
  }

  private StreamMetricsCollectorFactory createStreamMetricsCollectorFactory() {
    return new StreamMetricsCollectorFactory() {
      @Override
      public StreamMetricsCollector createMetricsCollector(final String streamName) {
        final MetricsCollector childCollector =
          metricsCollector.childCollector(Constants.Metrics.Tag.STREAM, streamName);
        return new StreamMetricsCollector() {
          @Override
          public void emitMetrics(long bytesWritten, long eventsWritten) {
            if (bytesWritten > 0) {
              childCollector.increment("collect.bytes", bytesWritten);
              sizeCollector.received(streamName, bytesWritten);
            }
            if (eventsWritten > 0) {
              childCollector.increment("collect.events", eventsWritten);
            }
          }
        };
      }
    };
  }

  private Map<String, String> getMetricsContext() {
    return ImmutableMap.of(Constants.Metrics.Tag.COMPONENT, Constants.Gateway.METRICS_CONTEXT,
                           Constants.Metrics.Tag.HANDLER, Constants.Gateway.STREAM_HANDLER_NAME,
                           Constants.Metrics.Tag.INSTANCE_ID, cConf.get(Constants.Stream.CONTAINER_INSTANCE_ID, "0"));
  }
  
  // given the current config for a stream and requested config for a stream, get the new stream config
  // with defaults in place of missing settings, and validation performed on the requested fields.
  // If a field is missing or invalid, this method will write an appropriate response to the responder and
  // return a null config.
  private StreamConfig getAndValidateConfig(StreamConfig currConfig, HttpRequest request, HttpResponder responder) {
    // get new config settings from the request. Only TTL and format spec can be changed, which is
    // why the StreamProperties object is used instead of a StreamConfig object.
    Reader reader = new InputStreamReader(new ChannelBufferInputStream(request.getContent()));
    StreamProperties properties;
    try {
      properties = GSON.fromJson(reader, StreamProperties.class);
    } catch (Exception e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid stream configuration. Please check that the " +
        "configuration is a valid JSON Object with a valid schema.");
      return null;
    }

    // if no ttl is given, use the existing ttl.
    Long newTTL = properties.getTTL();
    if (newTTL == null) {
      newTTL = currConfig.getTTL();
    } else {
      if (newTTL < 0) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "TTL value should be positive.");
        return null;
      }
      // TTL in the REST API is in seconds. Convert it to ms for the config.
      newTTL = TimeUnit.SECONDS.toMillis(newTTL);
    }

    FormatSpecification newFormatSpec = properties.getFormat();
    // if no format spec is given, use the existing format spec.
    if (newFormatSpec == null) {
      newFormatSpec = currConfig.getFormat();
    } else {
      String formatName = newFormatSpec.getName();
      if (formatName == null) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "A format name must be specified.");
        return null;
      }
      try {
        // if a format is given, make sure it is a valid format,
        // check that we can instantiate the format class
        RecordFormat format = RecordFormats.createInitializedFormat(newFormatSpec);
        // the request may contain a null schema, in which case the default schema of the format should be used.
        // create a new specification object that is guaranteed to have a non-null schema.
        newFormatSpec = new FormatSpecification(newFormatSpec.getName(),
                                                format.getSchema(), newFormatSpec.getSettings());
      } catch (UnsupportedTypeException e) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST,
                             "Format " + formatName + " does not support the requested schema.");
        return null;
      } catch (Exception e) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST,
                             "Invalid format, unable to instantiate format " + formatName);
        return null;
      }
    }

    return new StreamConfig(currConfig.getName(), currConfig.getPartitionDuration(), currConfig.getIndexInterval(),
                            newTTL, currConfig.getLocation(), newFormatSpec);
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
   * Creates a {@link ContentWriterFactory} based on the request size. Used by the batch endpoint.
   */
  private ContentWriterFactory createContentWriterFactory(String accountId,
                                                          String stream, HttpRequest request) throws IOException {
    long contentLength = HttpHeaders.getContentLength(request, -1L);
    if (contentLength >= 0 && contentLength <= batchBufferThreshold) {
      return new BufferedContentWriterFactory(accountId, stream, streamWriter, getHeaders(request, stream));
    }

    StreamConfig config = streamAdmin.getConfig(stream);
    return new FileContentWriterFactory(accountId, config, streamWriter, getHeaders(request, stream));
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
      json.add("format", context.serialize(src.getFormat(), FormatSpecification.class));
      return json;
    }
  }
}
