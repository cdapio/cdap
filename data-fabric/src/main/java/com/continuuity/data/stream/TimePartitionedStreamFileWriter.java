/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.data.file.FileWriter;
import com.continuuity.data.file.PartitionedFileWriter;
import com.continuuity.data.stream.TimePartitionedStreamFileWriter.TimePartition;
import com.google.common.io.OutputSupplier;
import com.google.common.primitives.Longs;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Stream file path format:
 *
 * <br/><br/>
 *   Each file has path pattern
 * <pre>
 *     [streamName]/[partitionName]/[bucketName].[dat|idx]
 * </pre>
 * Where {@code .dat} is the event data file, {@code .idx} is the accompany index file.
 *
 * <br/><br/>
 * The {@code partitionName} is formatted as
 * <pre>
 *   [partitionStartTime].[duration]
 * </pre>
 * with both {@code partitionStartTime} and {@code duration} in seconds.
 *
 * <br/><br/>
 * The {@code bucketName} is formatted as
 * <pre>
 *   "bucket".[bucketId].[seqNo]
 * </pre>
 * where the {@code bucketId} is an integer. The {@code seqNo} is a strictly increasing integer for the same
 * {@code bucketId}.
 */
@NotThreadSafe
public class TimePartitionedStreamFileWriter extends PartitionedFileWriter<StreamEvent, TimePartition> {

  private static final Logger LOG = LoggerFactory.getLogger(TimePartitionedStreamFileWriter.class);

  private final long partitionDuration;
  private TimePartition timePartition = new TimePartition(-1L);

  // TODO: Add a timer task to close file after duration has passed even there is no writer.

  public TimePartitionedStreamFileWriter(Location streamLocation, long partitionDuration,
                                         String fileNamePrefix, long indexInterval) {
    super(new StreamWriterFactory(streamLocation, partitionDuration, fileNamePrefix, indexInterval));
    this.partitionDuration = partitionDuration;
  }

  @Override
  protected TimePartition getPartition(StreamEvent event) {
    long eventPartitionStart = StreamUtils.getPartitionStartTime(event.getTimestamp(), partitionDuration);
    if (eventPartitionStart != timePartition.getStartTimestamp()) {
      timePartition = new TimePartition(eventPartitionStart);
    }
    return timePartition;
  }

  @Override
  protected void partitionChanged(TimePartition oldPartition, TimePartition newPartition) throws IOException {
    closePartitionWriter(oldPartition);
  }

  /**
   * Uses timestamp to represent partition information.
   */
  public static final class TimePartition {

    private final long startTimestamp;

    private TimePartition(long startTimestamp) {
      this.startTimestamp = startTimestamp;
    }

    private long getStartTimestamp() {
      return startTimestamp;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      TimePartition other = (TimePartition) o;
      return startTimestamp == other.startTimestamp;
    }

    @Override
    public int hashCode() {
      return Longs.hashCode(startTimestamp);
    }
  }


  private static final class StreamWriterFactory implements PartitionedFileWriterFactory<StreamEvent, TimePartition> {

    private final Location streamLocation;
    private final long partitionDuration;
    private final String fileNamePrefix;
    private final long indexInterval;

    StreamWriterFactory(Location streamLocation, long partitionDuration, String fileNamePrefix, long indexInterval) {
      this.streamLocation = streamLocation;
      this.partitionDuration = partitionDuration;
      this.fileNamePrefix = fileNamePrefix;
      this.indexInterval = indexInterval;
    }

    @Override
    public FileWriter<StreamEvent> create(TimePartition partition) throws IOException {
      long partitionStart = partition.getStartTimestamp();

      if (!streamLocation.isDirectory()) {
        throw new IOException("Stream " + streamLocation.getName() + " not exist in " + streamLocation.toURI());
      }

      Location partitionDirectory = StreamUtils.createPartitionLocation(streamLocation,
                                                                        partitionStart, partitionDuration);
      // Always try to create the directory
      partitionDirectory.mkdirs();

      // Try to find the file of this bucket with the highest sequence number.
      int maxSequence = -1;
      for (Location location : partitionDirectory.list()) {
        String fileName = location.getName();
        if (fileName.startsWith(fileNamePrefix)) {
          StreamUtils.getSequenceId(fileName);

          int idx = fileName.lastIndexOf('.');
          if (idx < fileNamePrefix.length()) {
            LOG.warn("Ignore file with invalid stream file name {}", location.toURI());
            continue;
          }

          try {
            // File name format is [prefix].[sequenceId].[dat|idx]
            int seq = StreamUtils.getSequenceId(fileName);
            if (seq > maxSequence) {
              maxSequence = seq;
            }
          } catch (NumberFormatException e) {
            LOG.warn("Ignore stream file with invalid sequence id {}", location.toURI());
          }
        }
      }

      // Create the event and index file with the max sequence + 1
      int fileSequence = maxSequence + 1;
      Location eventFile = StreamUtils.createStreamLocation(partitionDirectory, fileNamePrefix,
                                                            fileSequence, StreamFileType.EVENT);
      Location indexFile = StreamUtils.createStreamLocation(partitionDirectory, fileNamePrefix,
                                                            fileSequence, StreamFileType.INDEX);
      // The creation should succeed, as it's expected to only have one process running per fileNamePrefix.
      if (!eventFile.createNew() || !indexFile.createNew()) {
        throw new IOException("Failed to create new file at " + eventFile.toURI() + " and " + indexFile.toURI());
      }

      LOG.debug("New stream file created at {}", eventFile.toURI());
      return new StreamDataFileWriter(createOutputSupplier(eventFile), createOutputSupplier(indexFile), indexInterval);
    }

    private OutputSupplier<OutputStream> createOutputSupplier(final Location location) {
      return new OutputSupplier<OutputStream>() {
        @Override
        public OutputStream getOutput() throws IOException {
          return location.getOutputStream();
        }
      };
    }
  }
}
