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

import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.stream.StreamEventTypeAdapter;
import co.cask.cdap.data.file.FileReader;
import co.cask.cdap.data.file.ReadFilter;
import co.cask.cdap.data.stream.MultiLiveStreamFileReader;
import co.cask.cdap.data.stream.StreamEventOffset;
import co.cask.cdap.data.stream.StreamFileOffset;
import co.cask.cdap.data.stream.StreamFileType;
import co.cask.cdap.data.stream.StreamUtils;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.AuthenticatedHttpHandler;
import co.cask.http.ChunkResponder;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonWriter;
import com.google.inject.Inject;
import org.apache.twill.filesystem.Location;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * A HTTP handler for handling getting stream events.
 */
@Path(Constants.Gateway.API_VERSION_2 + "/streams")
public final class StreamFetchHandler extends AuthenticatedHttpHandler {

  private static final Gson GSON = StreamEventTypeAdapter.register(new GsonBuilder()).create();
  private static final int MAX_EVENTS_PER_READ = 100;
  private static final int CHUNK_SIZE = 20;

  private final CConfiguration cConf;
  private final StreamAdmin streamAdmin;
  private final StreamMetaStore streamMetaStore;

  @Inject
  public StreamFetchHandler(CConfiguration cConf, Authenticator authenticator,
                            StreamAdmin streamAdmin, StreamMetaStore streamMetaStore) {

    super(authenticator);
    this.cConf = cConf;
    this.streamAdmin = streamAdmin;
    this.streamMetaStore = streamMetaStore;
  }

  /**
   * Handler for the HTTP API {@code /streams/[stream_name]/events?start=[start_ts]&end=[end_ts]&limit=[event_limit]}
   *
   * Response with
   *   404 if stream not exists.
   *   204 if no event in the given start/end time range
   *   200 if there is event
   *
   * Response body is an Json array of StreamEvent object
   *
   * @see StreamEventTypeAdapter for the format of StreamEvent object.
   */
  @GET
  @Path("/{stream}/events")
  public void fetch(HttpRequest request, HttpResponder responder,
                        @PathParam("stream") String stream) throws Exception {

    String accountID = getAuthenticatedAccountId(request);

    Map<String, List<String>> parameters = new QueryStringDecoder(request.getUri()).getParameters();
    long startTime = getTimestamp("start", parameters, 0);
    long endTime = getTimestamp("end", parameters, Long.MAX_VALUE);
    int limit = getLimit("limit", parameters, Integer.MAX_VALUE);

    if (!verifyGetEventsRequest(accountID, stream, startTime, endTime, limit, responder)) {
      return;
    }

    StreamConfig streamConfig = streamAdmin.getConfig(stream);
    startTime = Math.max(startTime, System.currentTimeMillis() - streamConfig.getTTL());

    // Create the stream event reader
    FileReader<StreamEventOffset, Iterable<StreamFileOffset>> reader = createReader(streamConfig, startTime);
    try {
      ReadFilter readFilter = createReadFilter(startTime, endTime);
      List<StreamEvent> events = Lists.newArrayListWithCapacity(100);
      int eventsRead = reader.read(events, getReadLimit(limit), 0, TimeUnit.SECONDS, readFilter);

      // If empty already, return 204 no content
      if (eventsRead <= 0) {
        responder.sendStatus(HttpResponseStatus.NO_CONTENT);
        return;
      }

      // Send with chunk response, as we don't want to buffer all events in memory to determine the content-length.
      ChunkResponder chunkResponder = responder.sendChunkStart(HttpResponseStatus.OK,
                                                               ImmutableMultimap.of(HttpHeaders.Names.CONTENT_TYPE,
                                                                                    "application/json; charset=utf-8"));
      ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
      JsonWriter jsonWriter = new JsonWriter(new OutputStreamWriter(new ChannelBufferOutputStream(buffer),
                                                                    Charsets.UTF_8));
      // Response is an array of stream event
      jsonWriter.beginArray();
      while (limit > 0 && eventsRead > 0) {
        limit -= eventsRead;

        for (StreamEvent event : events) {
          GSON.toJson(event, StreamEvent.class, jsonWriter);
          jsonWriter.flush();

          // If exceeded chunk size limit, send a new chunk.
          if (buffer.readableBytes() >= CHUNK_SIZE) {
            // If the connect is closed, sendChunk will throw IOException.
            // No need to handle the exception as it will just propagated back to the netty-http library
            // and it will handle it.
            chunkResponder.sendChunk(buffer);
            buffer.clear();
          }
        }
        events.clear();

        if (limit > 0) {
          eventsRead = reader.read(events, getReadLimit(limit), 0, TimeUnit.SECONDS, readFilter);
        }
      }
      jsonWriter.endArray();
      jsonWriter.close();

      // Send the last chunk that still has data
      if (buffer.readable()) {
        chunkResponder.sendChunk(buffer);
      }
      Closeables.closeQuietly(chunkResponder);
    } finally {
      reader.close();
    }
  }

  /**
   * Parses and returns a timestamp from the query string.
   *
   * @param key Name of the key in the query string.
   * @param parameters The query string represented as a map.
   * @param defaultValue Value to return if the key is absent from the query string.
   * @return A long value parsed from the query string, or the {@code defaultValue} if the key is absent.
   *         If the key exists but fails to parse the number, {@code -1} is returned.
   */
  private long getTimestamp(String key, Map<String, List<String>> parameters, long defaultValue) {
    List<String> values = parameters.get(key);
    if (values == null || values.isEmpty()) {
      return defaultValue;
    }
    try {
      return Long.parseLong(values.get(0));
    } catch (NumberFormatException e) {
      return -1L;
    }
  }

  /**
   * Parses and returns the limit from the query string.
   *
   * @param key Name of the key in the query string.
   * @param parameters The query string represented as a map.
   * @param defaultValue Value to return if the key is absent from the query string.
   * @return An int value parsed from the query string, or the {@code defaultValue} if the key is absent.
   *         If the key exists but fails to parse the number, {@code -1} is returned.
   */
  private int getLimit(String key, Map<String, List<String>> parameters, int defaultValue) {
    List<String> values = parameters.get(key);
    if (values == null || values.isEmpty()) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(values.get(0));
    } catch (NumberFormatException e) {
      return -1;
    }
  }

  /**
   * Verifies query properties.
   */
  private boolean verifyGetEventsRequest(String accountID, String stream, long startTime, long endTime,
                                         int count, HttpResponder responder) throws Exception {
    if (startTime < 0) {
      responder.sendError(HttpResponseStatus.BAD_REQUEST, "Start time must be >= 0");
      return false;
    }
    if (endTime < 0) {
      responder.sendError(HttpResponseStatus.BAD_REQUEST, "End time must be >= 0");
      return false;
    }
    if (startTime >= endTime) {
      responder.sendError(HttpResponseStatus.BAD_REQUEST, "Start time must be smaller than end time");
      return false;
    }
    if (count <= 0) {
      responder.sendError(HttpResponseStatus.BAD_REQUEST, "Cannot request for <=0 events");
    }
    if (!streamMetaStore.streamExists(accountID, stream)) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      return false;
    }

    return true;
  }

  /**
   * Get the first partition location that contains events as specified in start time.
   *
   * @return the partition location if found, or {@code null} if not found.
   */
  private Location getStartPartitionLocation(StreamConfig streamConfig,
                                             long startTime, int generation) throws IOException {
    Location baseLocation = StreamUtils.createGenerationLocation(streamConfig.getLocation(), generation);

    // Find the partition location with the largest timestamp that is <= startTime
    // Also find the partition location with the smallest timestamp that is >= startTime
    long maxPartitionStartTime = 0L;
    long minPartitionStartTime = Long.MAX_VALUE;

    Location maxPartitionLocation = null;
    Location minPartitionLocation = null;

    for (Location location : baseLocation.list()) {
      // Partition must be a directory
      if (!location.isDirectory()) {
        continue;
      }
      long partitionStartTime = StreamUtils.getPartitionStartTime(location.getName());

      if (partitionStartTime > maxPartitionStartTime && partitionStartTime <= startTime) {
        maxPartitionStartTime = partitionStartTime;
        maxPartitionLocation = location;
      }
      if (partitionStartTime < minPartitionStartTime && partitionStartTime >= startTime) {
        minPartitionStartTime = partitionStartTime;
        minPartitionLocation = location;
      }
    }

    return maxPartitionLocation == null ? minPartitionLocation : maxPartitionLocation;
  }

  /**
   * Creates a {@link FileReader} that starts reading stream event from the given partition.
   */
  private FileReader<StreamEventOffset, Iterable<StreamFileOffset>> createReader(StreamConfig streamConfig,
                                                                                 long startTime) throws IOException {
    int generation = StreamUtils.getGeneration(streamConfig);
    Location startPartition = getStartPartitionLocation(streamConfig, startTime, generation);
    if (startPartition == null) {
      return createEmptyReader();
    }

    List<StreamFileOffset> fileOffsets = Lists.newArrayList();
    int instances = cConf.getInt(Constants.Stream.CONTAINER_INSTANCES);
    String filePrefix = cConf.get(Constants.Stream.FILE_PREFIX);
    for (int i = 0; i < instances; i++) {
      // The actual file prefix is formed by file prefix in cConf + writer instance id
      String streamFilePrefix = filePrefix + '.' + i;
      Location eventLocation = StreamUtils.createStreamLocation(startPartition, streamFilePrefix,
                                                                0, StreamFileType.EVENT);
      fileOffsets.add(new StreamFileOffset(eventLocation, 0, generation));
    }

    MultiLiveStreamFileReader reader = new MultiLiveStreamFileReader(streamConfig, fileOffsets);
    reader.initialize();
    return reader;
  }

  /**
   * Creates a reader that has no event to read.
   */
  private <T, P> FileReader<T, P> createEmptyReader() {
    return new FileReader<T, P>() {
      @Override
      public void initialize() throws IOException {
        // no-op
      }

      @Override
      public int read(Collection<? super T> events, int maxEvents,
                      long timeout, TimeUnit unit) throws IOException, InterruptedException {
        return -1;
      }

      @Override
      public int read(Collection<? super T> events, int maxEvents,
                      long timeout, TimeUnit unit, ReadFilter readFilter) throws IOException, InterruptedException {
        return -1;
      }

      @Override
      public void close() throws IOException {
        // no-op
      }

      @Override
      public P getPosition() {
        throw new UnsupportedOperationException("Position not supported for empty FileReader");
      }
    };
  }

  /**
   * Creates a {@link ReadFilter} to only read events that are within the given time range.
   *
   * @param startTime Start timestamp for event to be valid (inclusive).
   * @param endTime End timestamp fo event to be valid (exclusive).
   * @return A {@link ReadFilter} with the specific filtering property.
   */
  private ReadFilter createReadFilter(final long startTime, final long endTime) {
    return new ReadFilter() {

      private long hint;

      @Override
      public void reset() {
        hint = -1L;
      }

      @Override
      public long getNextTimestampHint() {
        return hint;
      }

      @Override
      public boolean acceptTimestamp(long timestamp) {
        if (timestamp < startTime) {
          hint = startTime;
          return false;
        }
        if (timestamp >= endTime) {
          hint = Long.MAX_VALUE;
          return false;
        }
        return true;
      }
    };
  }

  /**
   * Returns the events limit for each round of read from the stream reader.
   *
   * @param count Number of events wanted to read.
   * @return The actual number of events to read.
   */
  private int getReadLimit(int count) {
    return (count > MAX_EVENTS_PER_READ) ? MAX_EVENTS_PER_READ : count;
  }
}
