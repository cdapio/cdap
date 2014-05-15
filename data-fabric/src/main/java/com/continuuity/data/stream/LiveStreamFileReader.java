/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.io.Locations;
import com.continuuity.data.file.FileReader;
import com.continuuity.data.file.LiveFileReader;
import com.continuuity.data.file.ReadFilter;
import com.continuuity.data2.transaction.stream.StreamConfig;
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
  private final long newFileCheckInterval;
  private StreamPositionTransformFileReader reader;
  private long nextCheckTime = 0;

  /**
   * Construct a reader with {@link Constants.Stream#NEW_FILE_CHECK_INTERVAL} as the interval to check for new file.
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
   * @param streamConfig The stream configuration.
   * @param beginOffset The offset information to begin with.
   * @param newFileCheckInterval Interval in milliseconds for checking for new stream file.
   */
  public LiveStreamFileReader(StreamConfig streamConfig, StreamFileOffset beginOffset, long newFileCheckInterval) {
    this.streamConfig = streamConfig;
    this.beginOffset = beginOffset;
    this.newFileCheckInterval = newFileCheckInterval;
  }

  @Nullable
  @Override
  protected FileReader<PositionStreamEvent, StreamFileOffset> renewReader() throws IOException {
    // If no reader has yet opened, start with the beginning offset.
    if (reader == null) {
      reader = new StreamPositionTransformFileReader(streamConfig.getLocation(), beginOffset);
      reader.initialize();
      return reader;
    }

    StreamFileOffset offset = reader.getPosition();
    long now = System.currentTimeMillis();
    if (now < nextCheckTime && now < offset.getPartitionEnd()) {
      return null;
    }

    // See if newer file is available.
    nextCheckTime = now + newFileCheckInterval;

    // See if there is a higher sequence file available.
    StreamPositionTransformFileReader nextReader = createReader(reader.getPartitionLocation(),
                                                                offset.getNamePrefix(),
                                                                offset.getSequenceId() + 1, true);

    // If no higher sequence file and if current time is later than partition end time, see if there is new partition.
    if (nextReader == null && now > offset.getPartitionEnd()) {
      // Always create a new reader for the next partition when the current reader partition times up.
      Location partitionLocation = StreamUtils.createPartitionLocation(streamConfig.getLocation(),
                                                                       offset.getPartitionEnd(),
                                                                       streamConfig.getPartitionDuration());
      nextReader = createReader(partitionLocation, offset.getNamePrefix(), 0, false);
    }

    if (nextReader != null) {
      reader = nextReader;
    }

    return nextReader;
  }

  /**
   * Returns a new reader if that exists, or null.
   */
  private StreamPositionTransformFileReader createReader(Location partitionLocation, String prefix,
                                                         int seqId, boolean checkExists) throws IOException {

    Location eventLocation = StreamUtils.createStreamLocation(partitionLocation, prefix, seqId, StreamFileType.EVENT);

    if (checkExists && !eventLocation.exists()) {
      return null;
    }

    StreamPositionTransformFileReader reader =
      new StreamPositionTransformFileReader(streamConfig.getLocation(), new StreamFileOffset(eventLocation));
    reader.initialize();
    return reader;
  }

  @NotThreadSafe
  private static final class StreamPositionTransformFileReader
                       implements FileReader<PositionStreamEvent, StreamFileOffset> {

    private final FileReader<PositionStreamEvent, Long> reader;
    private final Location partitionLocation;
    private StreamFileOffset offset;

    private StreamPositionTransformFileReader(Location streamLocation, StreamFileOffset offset) throws IOException {
      this.reader = StreamDataFileReader.createWithOffset(Locations.newInputSupplier(offset.getEventLocation()),
                                                          Locations.newInputSupplier(offset.getIndexLocation()),
                                                          offset.getOffset());
      this.offset = new StreamFileOffset(offset);

      long duration = offset.getPartitionEnd() - offset.getPartitionStart();
      this.partitionLocation = StreamUtils.createPartitionLocation(streamLocation,
                                                                   offset.getPartitionStart(), duration);

      LOG.debug("Stream reader created for {}", offset.getEventLocation().toURI());
    }

    @Override
    public void initialize() throws IOException {
      LOG.info("Initialize stream reader {}", offset);
      reader.initialize();
      offset = new StreamFileOffset(offset, reader.getPosition());
      LOG.info("Stream reader initialized {}", offset);
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
