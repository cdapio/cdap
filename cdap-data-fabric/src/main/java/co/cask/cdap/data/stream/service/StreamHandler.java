/*
 * Copyright © 2015-2017 Cask Data, Inc.
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
import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.common.security.AuditDetail;
import co.cask.cdap.common.security.AuditPolicy;
import co.cask.cdap.data.stream.StreamCoordinatorClient;
import co.cask.cdap.data.stream.StreamFileWriterFactory;
import co.cask.cdap.data.stream.service.upload.ContentWriterFactory;
import co.cask.cdap.data.stream.service.upload.LengthBasedContentWriterFactory;
import co.cask.cdap.data.stream.service.upload.StreamBodyConsumerFactory;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.format.RecordFormats;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.cdap.proto.StreamDetail;
import co.cask.cdap.proto.StreamProperties;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.security.impersonation.Impersonator;
import co.cask.cdap.security.impersonation.SecurityUtil;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.BodyConsumer;
import co.cask.http.HandlerContext;
import co.cask.http.HttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closeables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.common.Threads;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * The {@link HttpHandler} for handling REST call to V3 stream APIs.
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}/streams")
public final class StreamHandler extends AbstractHttpHandler {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(StreamProperties.class, new StreamPropertiesAdapter())
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();

  private final CConfiguration cConf;
  private final StreamAdmin streamAdmin;
  private final MetricsContext streamHandlerMetricsContext;

  private final LoadingCache<NamespaceId, MetricsContext> streamMetricsCollectors;
  private final ConcurrentStreamWriter streamWriter;
  private final long batchBufferThreshold;
  private final StreamBodyConsumerFactory streamBodyConsumerFactory;
  private final NamespaceQueryAdmin namespaceQueryAdmin;

  // Executor for serving async enqueue requests
  private ExecutorService asyncExecutor;
  private final StreamWriterSizeCollector sizeCollector;
  private final Impersonator impersonator;
  private final AuthorizationEnforcer authorizationEnforcer;
  private final AuthenticationContext authenticationContext;

  @Inject
  StreamHandler(CConfiguration cConf,
                StreamCoordinatorClient streamCoordinatorClient, StreamAdmin streamAdmin,
                StreamFileWriterFactory writerFactory,
                final MetricsCollectionService metricsCollectionService,
                StreamWriterSizeCollector sizeCollector,
                NamespaceQueryAdmin namespaceQueryAdmin, Impersonator impersonator,
                AuthorizationEnforcer authorizationEnforcer, AuthenticationContext authenticationContext) {
    this.cConf = cConf;
    this.streamAdmin = streamAdmin;
    this.sizeCollector = sizeCollector;
    this.batchBufferThreshold = cConf.getLong(Constants.Stream.BATCH_BUFFER_THRESHOLD);
    this.streamBodyConsumerFactory = new StreamBodyConsumerFactory();
    this.streamHandlerMetricsContext = metricsCollectionService.getContext(getStreamHandlerMetricsContext());
    streamMetricsCollectors = CacheBuilder.newBuilder()
      .build(new CacheLoader<NamespaceId, MetricsContext>() {
        @Override
        public MetricsContext load(NamespaceId namespaceId) {
          return metricsCollectionService.getContext(getStreamMetricsContext(namespaceId));
        }
      });
    StreamMetricsCollectorFactory metricsCollectorFactory = createStreamMetricsCollectorFactory();
    this.streamWriter = new ConcurrentStreamWriter(streamCoordinatorClient, streamAdmin, writerFactory,
                                                   cConf.getInt(Constants.Stream.WORKER_THREADS),
                                                   metricsCollectorFactory, impersonator);
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.impersonator = impersonator;
    this.authorizationEnforcer = authorizationEnforcer;
    this.authenticationContext = authenticationContext;
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
  @Path("/")
  public void listStreams(HttpRequest request, HttpResponder responder,
                          @PathParam("namespace-id") String namespaceId) throws Exception {
    // Check for namespace existence. Throws NotFoundException if namespace doesn't exist
    namespaceQueryAdmin.get(new NamespaceId(namespaceId));
    List<StreamSpecification> specifications = streamAdmin.listStreams(new NamespaceId(namespaceId));
    List<StreamDetail> streamDetails = new ArrayList<>(specifications.size());
    for (StreamSpecification specification : specifications) {
      streamDetails.add(new StreamDetail(specification.getName()));
    }
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(streamDetails));
  }

  @GET
  @Path("/{stream}")
  public void getInfo(HttpRequest request, HttpResponder responder,
                      @PathParam("namespace-id") String namespaceId,
                      @PathParam("stream") String stream) throws Exception {
    StreamId streamId = validateAndGetStreamId(namespaceId, stream);
    checkStreamExists(streamId);
    StreamProperties properties = streamAdmin.getProperties(streamId);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(properties, StreamProperties.class));
  }

  @PUT
  @Path("/{stream}")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void create(FullHttpRequest request, HttpResponder responder,
                     @PathParam("namespace-id") String namespaceId,
                     @PathParam("stream") String stream) throws Exception {
    // Check for namespace existence. Throws NotFoundException if namespace doesn't exist
    namespaceQueryAdmin.get(new NamespaceId(namespaceId));

    StreamId streamId = validateAndGetStreamId(namespaceId, stream);

    Properties props = new Properties();
    StreamProperties streamProperties;
    // If the request to create a stream contains a non-empty body, then construct and set StreamProperties
    if (request.content().isReadable()) {
      streamProperties = getAndValidateConfig(request);

      if (streamProperties.getTTL() != null) {
        props.put(Constants.Stream.TTL, Long.toString(streamProperties.getTTL()));
      }

      if (streamProperties.getNotificationThresholdMB() != null) {
        props.put(Constants.Stream.NOTIFICATION_THRESHOLD,
                  Integer.toString(streamProperties.getNotificationThresholdMB()));
      }

      if (streamProperties.getDescription() != null) {
        props.put(Constants.Stream.DESCRIPTION, streamProperties.getDescription());
      }

      if (streamProperties.getFormat() != null) {
        props.put(Constants.Stream.FORMAT_SPECIFICATION, GSON.toJson(streamProperties.getFormat()));
      }

      if (streamProperties.getOwnerPrincipal() != null) {
        props.put(Constants.Security.PRINCIPAL, streamProperties.getOwnerPrincipal());
      }
    }

    streamAdmin.create(streamId, props);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/{stream}")
  public void enqueue(FullHttpRequest request, HttpResponder responder,
                      @PathParam("namespace-id") String namespaceId,
                      @PathParam("stream") String stream) throws Exception {
    StreamId streamId = validateAndGetStreamId(namespaceId, stream);
    authorizationEnforcer.enforce(streamId, authenticationContext.getPrincipal(), Action.WRITE);
    streamWriter.enqueue(streamId, getHeaders(request, stream), request.content().nioBuffer());
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/{stream}/async")
  public void asyncEnqueue(FullHttpRequest request, HttpResponder responder,
                           @PathParam("namespace-id") String namespaceId,
                           @PathParam("stream") String stream) throws Exception {
    StreamId streamId = validateAndGetStreamId(namespaceId, stream);
    // No need to copy the content buffer as we always uses a ChannelBufferFactory that won't reuse buffer.
    // See StreamHttpService
    authorizationEnforcer.enforce(streamId, authenticationContext.getPrincipal(), Action.WRITE);

    // Need to retain the content buffer since writing is async
    streamWriter.asyncEnqueue(streamId, getHeaders(request, stream), request.content().retain(), asyncExecutor);
    responder.sendStatus(HttpResponseStatus.ACCEPTED);
  }

  @POST
  @Path("/{stream}/batch")
  public BodyConsumer batch(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("stream") String stream) throws Exception {
    StreamId streamId = validateAndGetStreamId(namespaceId, stream);
    checkStreamExists(streamId);
    authorizationEnforcer.enforce(streamId, authenticationContext.getPrincipal(), Action.WRITE);
    try {
      return streamBodyConsumerFactory.create(request, createContentWriterFactory(streamId, request));
    } catch (UnsupportedOperationException e) {
      responder.sendString(HttpResponseStatus.NOT_ACCEPTABLE, e.getMessage());
      return null;
    }
  }

  @POST
  @Path("/{stream}/truncate")
  public void truncate(HttpRequest request, HttpResponder responder,
                       @PathParam("namespace-id") String namespaceId,
                       @PathParam("stream") String stream) throws Exception {
    StreamId streamId = validateAndGetStreamId(namespaceId, stream);
    checkStreamExists(streamId);

    streamAdmin.truncate(streamId);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @DELETE
  @Path("/{stream}")
  public void delete(HttpRequest request, HttpResponder responder,
                     @PathParam("namespace-id") String namespaceId,
                     @PathParam("stream") String stream) throws Exception {
    deleteStream(namespaceId, stream);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  public void deleteStream(String namespaceId, String stream) throws Exception {
    StreamId streamId = validateAndGetStreamId(namespaceId, stream);
    checkStreamExists(streamId);
    // On Windows, we can not move the file if it is open, and the stream writer may have an open file in this dir.
    // Since Windows is only supported in SDK/standalone, we don't need to worry about multiple stream writers here.
    streamWriter.close(streamId);
    streamAdmin.drop(streamId);
  }

  @PUT
  @Path("/{stream}/properties")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void setConfig(FullHttpRequest request, HttpResponder responder,
                        @PathParam("namespace-id") String namespaceId,
                        @PathParam("stream") String stream) throws Exception {
    StreamId streamId = validateAndGetStreamId(namespaceId, stream);
    checkStreamExists(streamId);

    StreamProperties properties = getAndValidateConfig(request);

    streamAdmin.updateConfig(streamId, properties);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  private void checkStreamExists(StreamId streamId) throws Exception {
    if (!streamAdmin.exists(streamId)) {
      throw new NotFoundException(streamId);
    }
  }

  private StreamMetricsCollectorFactory createStreamMetricsCollectorFactory() {
    return new StreamMetricsCollectorFactory() {
      @Override
      public StreamMetricsCollector createMetricsCollector(final StreamId streamId) {
        MetricsContext streamMetricsContext = streamMetricsCollectors.getUnchecked(streamId.getParent());
        final MetricsContext childCollector =
          streamMetricsContext.childContext(Constants.Metrics.Tag.STREAM, streamId.getEntityName());
        return new StreamMetricsCollector() {
          @Override
          public void emitMetrics(long bytesWritten, long eventsWritten) {
            if (bytesWritten > 0) {
              childCollector.increment("collect.bytes", bytesWritten);
              sizeCollector.received(streamId, bytesWritten);
            }
            if (eventsWritten > 0) {
              childCollector.increment("collect.events", eventsWritten);
            }
          }
        };
      }
    };
  }

  private Map<String, String> getStreamHandlerMetricsContext() {
    return ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, NamespaceId.SYSTEM.getNamespace(),
                           Constants.Metrics.Tag.COMPONENT, Constants.Gateway.METRICS_CONTEXT,
                           Constants.Metrics.Tag.HANDLER, Constants.Gateway.STREAM_HANDLER_NAME,
                           Constants.Metrics.Tag.INSTANCE_ID, cConf.get(Constants.Stream.CONTAINER_INSTANCE_ID, "0"));
  }

  private Map<String, String> getStreamMetricsContext(NamespaceId namespaceId) {
    return ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, namespaceId.getNamespace(),
                           Constants.Metrics.Tag.COMPONENT, Constants.Gateway.METRICS_CONTEXT,
                           Constants.Metrics.Tag.HANDLER, Constants.Gateway.STREAM_HANDLER_NAME,
                           Constants.Metrics.Tag.INSTANCE_ID, cConf.get(Constants.Stream.CONTAINER_INSTANCE_ID, "0"));
  }

  /**
   * Gets stream properties from the request. If there is request is invalid, a BadRequestException will be thrown.
   */
  private StreamProperties getAndValidateConfig(FullHttpRequest request) throws BadRequestException {
    StreamProperties properties;

    try (Reader reader = new InputStreamReader(new ByteBufInputStream(request.content()), StandardCharsets.UTF_8)) {
      properties = GSON.fromJson(reader, StreamProperties.class);
    } catch (Exception e) {
      throw new BadRequestException("Invalid stream configuration. Please check that the " +
                                      "configuration is a valid JSON Object with a valid schema. Cause: " +
                                      e.getMessage());
    }

    // Validate ttl
    Long ttl = properties.getTTL();
    if (ttl != null && ttl < 0) {
      throw new BadRequestException("Invalid TTL " + ttl + ". TTL value should be positive.");
    }

    // Validate format
    FormatSpecification formatSpec = properties.getFormat();
    if (formatSpec != null) {
      String formatName = formatSpec.getName();
      if (formatName == null) {
        throw new BadRequestException("A format name must be specified.");
      }
      try {
        // if a format is given, make sure it is a valid format,
        // check that we can instantiate the format class
        RecordFormat<?, ?> format = RecordFormats.createInitializedFormat(formatSpec);
        // the request may contain a null schema, in which case the default schema of the format should be used.
        // create a new specification object that is guaranteed to have a non-null schema.
        formatSpec = new FormatSpecification(formatSpec.getName(),
                                                format.getSchema(), formatSpec.getSettings());
      } catch (UnsupportedTypeException e) {
        throw new BadRequestException("Format " + formatName + " does not support the requested schema.");
      } catch (Exception e) {
        throw new BadRequestException("Invalid format, unable to instantiate format " + formatName);
      }
    }

    // Validate notification threshold
    Integer threshold = properties.getNotificationThresholdMB();
    if (threshold != null && threshold <= 0) {
      throw new BadRequestException(
        "Invalid threshold " + threshold + ". Threshold value should be greater than zero.");
    }

    // validate owner principal if one is provided
    if (properties.getOwnerPrincipal() != null) {
      SecurityUtil.validateKerberosPrincipal(properties.getOwnerPrincipal());
    }

    return new StreamProperties(ttl, formatSpec, threshold, properties.getDescription(),
                                properties.getOwnerPrincipal());
  }

  private RejectedExecutionHandler createAsyncRejectedExecutionHandler() {
    return new RejectedExecutionHandler() {
      @Override
      public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        if (!executor.isShutdown()) {
          streamHandlerMetricsContext.increment("collect.async.reject", 1);
          r.run();
        }
      }
    };
  }

  /**
   * Same as calling {@link #getHeaders(HttpRequest, String, ImmutableMap.Builder)} with a new builder.
   */
  private Map<String, String> getHeaders(HttpRequest request, String stream) {
    return getHeaders(request, stream, ImmutableMap.<String, String>builder());
  }

  /**
   * Extracts event headers from the HTTP request. Only HTTP headers that are prefixed with "{@code <stream-name>.}"
   * will be included. The result will be stored in an Immutable map built by the given builder.
   */
  private Map<String, String> getHeaders(HttpRequest request, String stream,
                                         ImmutableMap.Builder<String, String> builder) {
    // and transfer all other headers that are to be preserved
    String prefix = stream + ".";
    for (Map.Entry<String, String> header : request.headers()) {
      if (header.getKey().startsWith(prefix)) {
        builder.put(header.getKey().substring(prefix.length()), header.getValue());
      }
    }
    return builder.build();
  }

  /**
   * Creates a {@link ContentWriterFactory} based on the request size. Used by the batch endpoint.
   */
  private ContentWriterFactory createContentWriterFactory(StreamId streamId, HttpRequest request) throws IOException {
    String contentType = request.headers().get(HttpHeaderNames.CONTENT_TYPE, "");

    // The content-type is guaranteed to be non-empty, otherwise the batch request itself will fail.
    Map<String, String> headers = getHeaders(request, streamId.getEntityName(),
                                             ImmutableMap.<String, String>builder().put("content.type", contentType));

    StreamConfig config = streamAdmin.getConfig(streamId);
    return new LengthBasedContentWriterFactory(config, streamWriter, headers, batchBufferThreshold, impersonator);
  }

  /**
   *  Adapter class for {@link StreamProperties}. Its main purpose is to transform
   *  the unit of TTL, which is second in JSON, but millisecond in the StreamProperties object.
   */
  private static final class StreamPropertiesAdapter implements JsonSerializer<StreamProperties>,
                                                                JsonDeserializer<StreamProperties> {
    @Override
    public JsonElement serialize(StreamProperties src, Type typeOfSrc, JsonSerializationContext context) {
      JsonObject json = new JsonObject();
      if (src.getTTL() != null) {
        json.addProperty("ttl", TimeUnit.MILLISECONDS.toSeconds(src.getTTL()));
      }
      if (src.getFormat() != null) {
        json.add("format", context.serialize(src.getFormat(), FormatSpecification.class));
      }
      if (src.getNotificationThresholdMB() != null) {
        json.addProperty("notification.threshold.mb", src.getNotificationThresholdMB());
      }
      if (src.getDescription() != null) {
        json.addProperty("description", src.getDescription());
      }
      if (src.getOwnerPrincipal() != null) {
        json.addProperty(Constants.Security.PRINCIPAL, src.getOwnerPrincipal());
      }
      return json;
    }

    @Override
    public StreamProperties deserialize(JsonElement json, Type typeOfT,
                                        JsonDeserializationContext context) throws JsonParseException {
      JsonObject jsonObj = json.getAsJsonObject();
      Long ttl = jsonObj.has("ttl") ? TimeUnit.SECONDS.toMillis(jsonObj.get("ttl").getAsLong()) : null;

      FormatSpecification format = null;
      if (jsonObj.has("format")) {
        format = context.deserialize(jsonObj.get("format"), FormatSpecification.class);
      }

      Integer threshold = jsonObj.has("notification.threshold.mb") ?
        jsonObj.get("notification.threshold.mb").getAsInt() : null;

      String description = jsonObj.has("description") ? jsonObj.get("description").getAsString() : null;
      String ownerPrincipal = jsonObj.has(Constants.Security.PRINCIPAL) ?
        jsonObj.get(Constants.Security.PRINCIPAL).getAsString() : null;
      return new StreamProperties(ttl, format, threshold, description, ownerPrincipal);
    }
  }

  private StreamId validateAndGetStreamId(String namespace, String stream) throws BadRequestException {
    try {
      // Verify stream name
      return new StreamId(namespace, stream);
    } catch (IllegalArgumentException | NullPointerException e) {
      throw new BadRequestException(e);
    }
  }
}
