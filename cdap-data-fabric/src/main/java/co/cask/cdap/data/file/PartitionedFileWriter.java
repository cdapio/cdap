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
package co.cask.cdap.data.file;

import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.PeekingIterator;
import com.google.common.io.Closeables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * An abstract base class for implementation partitioned {@link FileWriter}.
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
      getWriter(event).append(event);
    } catch (Throwable t) {
      LOG.error("Exception on append.", t);
      Closeables.closeQuietly(this);
      Throwables.propagateIfInstanceOf(t, IOException.class);
      throw Throwables.propagate(t);
    }
  }

  @Override
  public void appendAll(final Iterator<? extends T> events) throws IOException {
    if (closed) {
      throw new IOException("Attempts to write to a closed FileWriter.");
    }

    PeekingIterator<T> iterator = Iterators.peekingIterator(events);
    while (iterator.hasNext()) {
      getWriter(iterator.peek()).appendAll(new AppendIterator(iterator));
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
   * Gets a {@link FileWriter} for the given event.
   */
  private FileWriter<T> getWriter(T event) throws IOException {
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

    return currentWriter;
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

  /**
   * An {@link Iterator} to support the {@link PartitionedFileWriter#appendAll(Iterator)} operation.
   * It will write as many events as possible as long as the event partition stays the same.
   */
  private final class AppendIterator extends AbstractIterator<T> {

    private final PeekingIterator<? extends T> events;

    AppendIterator(PeekingIterator<? extends T> events) {
      this.events = events;
    }

    @Override
    protected T computeNext() {
      if (!events.hasNext()) {
        return endOfData();
      }

      T event = events.peek();
      P partition = getPartition(event);
      if (Objects.equal(currentPartition, partition)) {
        events.next();
        return event;
      }
      return endOfData();
    }
  }
}
