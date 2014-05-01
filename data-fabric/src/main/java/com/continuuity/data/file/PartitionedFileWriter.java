/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.file;

import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;

/**
 *
 * @param <T> Type of event.
 * @param <P> Type of partition.
 */
@NotThreadSafe
public abstract class PartitionedFileWriter<T, P> implements FileWriter<T> {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionedFileWriter.class);

  private final PartitionedFileWriterFactory<T, P> fileWriterFactory;
  private final Map<P, FileWriter<T>> writers;
  private P currentPartition;
  private FileWriter<T> currentWriter;
  private boolean closed;

  /**
   * Constructs with the given file writer factory.
   */
  protected PartitionedFileWriter(PartitionedFileWriterFactory<T, P> fileWriterFactory) {
    this.fileWriterFactory = fileWriterFactory;
    this.writers = Maps.newHashMap();
  }


  @Override
  public void append(T event) throws IOException {
    if (closed) {
      throw new IOException("Attempts to write to a closed FileWriter.");
    }

    try {
      P partition = getPartition(event);

      // If the partition changed, get the file writer for the new partition.
      if (!Objects.equal(currentPartition, partition)) {
        // Notify that partition changed, so that children class can take action if necessary.
        partitionChanged(currentPartition, partition);
        currentWriter = writers.get(partition);
        if (currentWriter == null) {
          currentWriter = fileWriterFactory.create(partition);
          writers.put(partition, currentWriter);
        }
        currentPartition = partition;
      }

      currentWriter.append(event);

    } catch (Throwable t) {
      try {
        LOG.error("Exception on append.", t);
        if (t instanceof IOException) {
          throw (IOException) t;
        } else {
          throw new IOException(t);
        }
      } finally {
        close();
      }
    }
  }

  @Override
  public void flush() throws IOException {
    // Special case to avoid looping the map values
    if (writers.size() == 1) {
      currentWriter.flush();
    } else if (!writers.isEmpty()) {
      IOException flushException = null;
      for (FileWriter<T> writer : writers.values()) {
        try {
          writer.flush();
        } catch (IOException e) {
          flushException = e;
        }
      }

      if (flushException != null) {
        throw flushException;
      }
    }
  }

  @Override
  public void close() throws IOException {
    try {
      IOException closeException = null;
      for (FileWriter<T> writer : writers.values()) {
        try {
          writer.close();
        } catch (IOException e) {
          // Catch the exception and throw after the loop as we want to close all writers.
          closeException = e;
        }
      }
      if (closeException != null) {
        throw closeException;
      }
    } finally {
      closed = true;
    }
  }

  /**
   * Closes the {@link FileWriter} for the given partition.
   */
  protected final void closePartitionWriter(P partition) throws IOException {
    FileWriter<T> writer = writers.remove(partition);
    if (writer != null) {
      writer.close();
    }
  }

  /**
   * Invoked when partition changed (different from the last event get appended).
   */
  protected void partitionChanged(P oldPartition, P newPartition) throws IOException {
    // No-op by default.
  }

  /**
   * Returns the partition ID for the given event.
   *
   * @param event The event for computing a partition ID.
   * @return A long representing the partition ID.
   */
  protected abstract P getPartition(T event);

  /**
   *
   * @param <T> Type of event that can be appended to the {@link FileWriter} created by this factory.
   * @param <P> Type of partition.
   */
  protected interface PartitionedFileWriterFactory<T, P> {

    /**
     * Creates a {@link FileWriter} for writing event of type {@code T} for the given partition.
     *
     * @param partition Partition information.
     * @return A {@link FileWriter}.
     * @throws java.io.IOException If creation failed.
     */
    FileWriter<T> create(P partition) throws IOException;
  }
}
