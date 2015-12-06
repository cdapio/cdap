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
package co.cask.cdap.data.stream;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.data.file.FileReader;
import co.cask.cdap.data.file.LiveFileReader;
import co.cask.cdap.data.file.ReadFilter;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A {@link LiveFileReader} that gives infinite stream event.
 */
public final class LiveStreamFileReader extends LiveFileReader<PositionStreamEvent, StreamFileOffset> {

  private static final Logger LOG = LoggerFactory.getLogger(LiveStreamFileReader.class);

  private final StreamFileOffset beginOffset;
  private final StreamConfig streamConfig;
  private final long maxFileCheckInterval;
  private StreamPositionTransformFileReader reader;
  private int retries;
  private long nextCheckTime = 0;

  /**
   * Construct a reader with {@link Constants.Stream#NEW_FILE_CHECK_INTERVAL} as the max interval to check for new file.
   *
   * <p/>
   * Same as calling
   * {@link #LiveStreamFileReader(StreamConfig, StreamFileOffset, long)
   * LiveStreamFileReader(streamConfig, beginOffset, Constants.Stream.NEW_FILE_CHECK_INTERVAL).
   * }
   */
  public LiveStreamFileReader(StreamConfig streamConfig, StreamFileOffset beginOffset) {
    this(streamConfig, beginOffset, Constants.Stream.NEW_FILE_CHECK_INTERVAL);
  }

  /**
   * Creates a new file reader.
   *
   * @param streamConfig the stream configuration.
   * @param beginOffset the offset information to begin with.
   * @param maxFileCheckInterval maximum interval in milliseconds for checking for new stream file.
   */
  public LiveStreamFileReader(StreamConfig streamConfig, StreamFileOffset beginOffset, long maxFileCheckInterval) {
    this.streamConfig = streamConfig;
    this.beginOffset = beginOffset;
    this.maxFileCheckInterval = (maxFileCheckInterval <= 0) ? Constants.Stream.NEW_FILE_CHECK_INTERVAL
                                                            : maxFileCheckInterval;
  }

  @Nullable
  @Override
  protected FileReader<PositionStreamEvent, StreamFileOffset> renewReader() throws IOException {
    // If no reader has yet opened, start with the beginning offset.
    if (reader == null) {
      reader = new StreamPositionTransformFileReader(beginOffset);
      reader.initialize();
      return reader;
    }

    StreamFileOffset offset = reader.getPosition();
    long now = System.currentTimeMillis();
    if (now < nextCheckTime && now < offset.getPartitionEnd()) {
      return null;
    }

    // See if there is a higher sequence file available.
    Location eventLocation = StreamUtils.createStreamLocation(reader.getPartitionLocation(),
                                                              offset.getNamePrefix(),
                                                              offset.getSequenceId() + 1,
                                                              StreamFileType.EVENT);

    StreamPositionTransformFileReader nextReader = createReader(eventLocation, true, offset.getGeneration());

    // If no higher sequence file and if current time is later than partition end time, see if there is new partition.
    if (nextReader == null && now > offset.getPartitionEnd()) {
      // Always create a new reader for the next partition when the current reader partition times up.
      eventLocation = StreamUtils.createStreamLocation(
        StreamUtils.createPartitionLocation(Locations.getParent(reader.getPartitionLocation()),
                                            offset.getPartitionEnd(), streamConfig.getPartitionDuration()),
        offset.getNamePrefix(), 0, StreamFileType.EVENT);

      nextReader = createReader(eventLocation, false, offset.getGeneration());
    }

    if (nextReader != null) {
      reader = nextReader;
      retries = 0;
      nextCheckTime = 0;
    } else {
      // Update the next check time to the next check interval if there is no new file found.
      // This is for reducing load to the file system.
      nextCheckTime = now + getCheckInterval();
    }

    return nextReader;
  }

  /**
   * Computes the new file check interval based on number of retries that a new file hasn't been found.
   */
  private long getCheckInterval() {
    retries = Math.min((retries + 1), Integer.SIZE);
    int multiplier = retries >= Integer.SIZE ? Integer.MAX_VALUE : (1 << (retries - 1));
    return Math.min(100L * multiplier, maxFileCheckInterval);
  }

  /**
   * Returns a new reader if that exists, or null, unless checkExists is false.
   */
  private StreamPositionTransformFileReader createReader(Location eventLocation,
                                                         boolean checkExists,
                                                         int generation) throws IOException {
    if (checkExists && !eventLocation.exists()) {
      return null;
    }

    StreamPositionTransformFileReader reader =
      new StreamPositionTransformFileReader(new StreamFileOffset(eventLocation, 0L, generation));
    reader.initialize();
    return reader;
  }

  @NotThreadSafe
  private static final class StreamPositionTransformFileReader
                       implements FileReader<PositionStreamEvent, StreamFileOffset> {

    private final FileReader<PositionStreamEvent, Long> reader;
    private final Location partitionLocation;
    private StreamFileOffset offset;

    private StreamPositionTransformFileReader(StreamFileOffset offset) throws IOException {
      this.reader = StreamDataFileReader.createWithOffset(Locations.newInputSupplier(offset.getEventLocation()),
                                                          Locations.newInputSupplier(offset.getIndexLocation()),
                                                          offset.getOffset());
      this.offset = new StreamFileOffset(offset);
      this.partitionLocation = Locations.getParent(offset.getEventLocation());

      LOG.trace("Stream reader created for {}", offset.getEventLocation().toURI());
    }

    @Override
    public void initialize() throws IOException {
      LOG.trace("Initialize stream reader {}", offset);
      reader.initialize();
      offset = new StreamFileOffset(offset, reader.getPosition());
      LOG.trace("Stream reader initialized {}", offset);
    }

    @Override
    public int read(Collection<? super PositionStreamEvent> events, int maxEvents,
                    long timeout, TimeUnit unit) throws IOException, InterruptedException {
      return read(events, maxEvents, timeout, unit, ReadFilter.ALWAYS_ACCEPT);
    }

    @Override
    public int read(Collection<? super PositionStreamEvent> events, int maxEvents,
                    long timeout, TimeUnit unit, ReadFilter readFilter) throws IOException, InterruptedException {
      int eventCount = reader.read(events, maxEvents, timeout, unit, readFilter);
      offset = new StreamFileOffset(offset, reader.getPosition());
      return eventCount;
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }

    @Override
    public StreamFileOffset getPosition() {
      return offset;
    }

    Location getPartitionLocation() {
      return partitionLocation;
    }
  }
}
