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
import co.cask.cdap.proto.Id;
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

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * A HTTP handler for handling getting stream events.
 */
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}/streams")
public final class StreamFetchHandler extends AuthenticatedHttpHandler {

  private static final Gson GSON = StreamEventTypeAdapter.register(new GsonBuilder()).create();
  private static final int MAX_EVENTS_PER_READ = 100;
  private static final int CHUNK_SIZE = 8192;

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
   * <p/>
   * Response with
   * 404 if stream not exists.
   * 204 if no event in the given start/end time range
   * 200 if there is event
   * <p/>
   * Response body is an Json array of StreamEvent object
   *
   * @see StreamEventTypeAdapter for the format of StreamEvent object.
   */
  @GET
  @Path("/{stream}/events")
  public void fetch(HttpRequest request, HttpResponder responder,
                    @PathParam("namespace-id") String namespaceId,
                    @PathParam("stream") String stream,
                    @QueryParam("start") long startTime,
                    @QueryParam("end") @DefaultValue("9223372036854775807") long endTime,
                    @QueryParam("limit") @DefaultValue("2147483647") int limit) throws Exception {

    String accountID = getAuthenticatedAccountId(request);
    if (!verifyGetEventsRequest(accountID, stream, startTime, endTime, limit, responder)) {
      return;
    }

    Id.Stream streamId = Id.Stream.from(accountID, stream);
    StreamConfig streamConfig = streamAdmin.getConfig(streamId);
    long now = System.currentTimeMillis();
    startTime = Math.max(startTime, now - streamConfig.getTTL());
    endTime = Math.min(endTime, now);

    // Create the stream event reader
    FileReader<StreamEventOffset, Iterable<StreamFileOffset>> reader = createReader(streamConfig, startTime);
    try {
      TimeRangeReadFilter readFilter = new TimeRangeReadFilter(startTime, endTime);
      List<StreamEvent> events = Lists.newArrayListWithCapacity(100);

      // Reads the first batch of events from the stream.
      int eventsRead = readEvents(reader, events, limit, readFilter);

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
            // Need to copy the buffer because the buffer will get reused and send chunk is an async operation
            chunkResponder.sendChunk(buffer.copy());
            buffer.clear();
          }
        }
        events.clear();

        if (limit > 0) {
          eventsRead = readEvents(reader, events, limit, readFilter);
        }
      }
      jsonWriter.endArray();
      jsonWriter.close();

      // Send the last chunk that still has data
      if (buffer.readable()) {
        // No need to copy the last chunk, since the buffer will not be reused
        chunkResponder.sendChunk(buffer);
      }
      Closeables.closeQuietly(chunkResponder);
    } finally {
      reader.close();
    }
  }

  /**
   * Reads events from the given reader.
   */
  private int readEvents(FileReader<StreamEventOffset, Iterable<StreamFileOffset>> reader,
                         List<StreamEvent> events, int limit,
                         TimeRangeReadFilter readFilter) throws IOException, InterruptedException {
    // Keeps reading as long as the filter is active.
    // This mean there are events in the stream, just that they are rejected by the filter.
    int eventsRead = reader.read(events, getReadLimit(limit), 0, TimeUnit.SECONDS, readFilter);
    while (eventsRead == 0 && readFilter.isActive()) {
      readFilter.reset();
      eventsRead = reader.read(events, getReadLimit(limit), 0, TimeUnit.SECONDS, readFilter);
    }
    return eventsRead;
  }

  /**
   * Verifies query properties.
   */
  private boolean verifyGetEventsRequest(String accountID, String stream, long startTime, long endTime,
                                         int count, HttpResponder responder) throws Exception {
    if (startTime < 0) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Start time must be >= 0");
      return false;
    }
    if (endTime < 0) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "End time must be >= 0");
      return false;
    }
    if (startTime >= endTime) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Start time must be smaller than end time");
      return false;
    }
    if (count <= 0) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Cannot request for <=0 events");
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
      if (!location.isDirectory() || !StreamUtils.isPartition(location.getName())) {
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
   * Returns the events limit for each round of read from the stream reader.
   *
   * @param count Number of events wanted to read.
   * @return The actual number of events to read.
   */
  private int getReadLimit(int count) {
    return (count > MAX_EVENTS_PER_READ) ? MAX_EVENTS_PER_READ : count;
  }

  /**
   * A {@link ReadFilter} for accepting events that are within a given time range.
   */
  private static final class TimeRangeReadFilter extends ReadFilter {
    private final long startTime;
    private final long endTime;
    private long hint;
    private boolean active;

    /**
     * Creates a {@link TimeRangeReadFilter} with the specific time range.
     *
     * @param startTime start timestamp for event to be accepted (inclusive)
     * @param endTime end timestamp for event to be accepted (exclusive)
     */
    private TimeRangeReadFilter(long startTime, long endTime) {
      this.startTime = startTime;
      this.endTime = endTime;
    }

    @Override
    public void reset() {
      hint = -1L;
      active = false;
    }

    @Override
    public long getNextTimestampHint() {
      return hint;
    }

    @Override
    public boolean acceptTimestamp(long timestamp) {
      if (timestamp < startTime) {
        // Reading of stream events is always sorted by timestamp
        // If the timestamp read is still smaller than the start time, there is still chance
        // that there will be events that can satisfy this filter, hence needs to keep reading.
        active = true;
        hint = startTime;
        return false;
      }
      if (timestamp >= endTime) {
        // If the timestamp read already passed the end time, further reading will not get any more events that
        // can satisfy this filter.
        active = false;
        hint = Long.MAX_VALUE;
        return false;
      }
      active = true;
      return true;
    }

    /**
     * Returns true if this filter has been called at least once after the prior call to {@link #reset()}.
     */
    public boolean isActive() {
      return active;
    }
  }
}
