/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.messaging.service;

import co.cask.cdap.common.utils.TimeProvider;
import co.cask.cdap.messaging.StoreRequest;
import co.cask.cdap.proto.id.TopicId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;
import com.google.common.util.concurrent.Uninterruptibles;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * An abstract base class to abstract out writing of {@link Iterator} of {@link StoreRequest} to underlying
 * storage table.
 *
 * @param <T> Type of the internal entry that can be written by this writer.
 */
@NotThreadSafe
abstract class StoreRequestWriter<T> implements Closeable {

  // Sequence ID has 2 bytes, which the max value is 65535 (unsigned short).
  // Hence we cannot have more than 65536 messages with the same timestamp
  @VisibleForTesting
  static final int SEQUENCE_ID_LIMIT = 0x10000;

  private final TimeProvider timeProvider;
  private long writeTimestamp;
  private long lastWriteTimestamp;
  private int seqId;

  private final PayloadTransformIterator payloadTransformIterator;

  /**
   * Constructor.
   *
   * @param timeProvider the {@link TimeProvider} for generating timestamp to be used for write timestamp
   * @param generateNullPayloadEntry {@code true} to generate table entry with {@code null} payload if
   *                                 a {@link PendingStoreRequest} has an empty iterator of payload.
   */
  StoreRequestWriter(TimeProvider timeProvider, boolean generateNullPayloadEntry) {
    this.timeProvider = timeProvider;
    this.payloadTransformIterator = new PayloadTransformIterator(generateNullPayloadEntry);
  }

  /**
   * Writes the given list of {@link PendingStoreRequest} through this writer.
   */
  final void write(final Iterator<? extends PendingStoreRequest> requests) throws IOException {
    // Make sure we start with the current timestamp
    updateTimeSequence();

    // Transform payloads inside each PendingStoreRequest into individual write entry
    doWrite(new AbstractIterator<T>() {
      private PendingStoreRequest currentRequest;

      @Override
      protected T computeNext() {
        // If there is no current payload iterator (initial case)
        // or the current iterator is already exhausted (finished one PendingStoreRequest),
        // look for a new payload iterator from the next PendingStoreRequest
        while (!payloadTransformIterator.hasNext()) {
          // Finished all PendingStoreRequest
          if (!requests.hasNext()) {
            return endOfData();
          }

          // Get the next PendingStoreRequest
          currentRequest = requests.next();
          payloadTransformIterator.reset(currentRequest);
        }
        return payloadTransformIterator.hasNext() ? payloadTransformIterator.next() : endOfData();
      }
    });
  }

  /**
   * Returns an entry to be written based on the provided information.
   *
   * @param topicId the topic id
   * @param transactional whether a store request is transactional or not
   * @param transactionWritePointer the transaction write pointer if the request is transactional
   * @param writeTimestamp the timestamp to be used as the write timestamp
   * @param sequenceId the sequence id to be used
   * @param payload the message payload
   * @return an entry of type {@code <T>}.
   */
  abstract T getEntry(TopicId topicId, boolean transactional, long transactionWritePointer,
                      long writeTimestamp, short sequenceId, @Nullable byte[] payload);

  /**
   * Writes the given list of entries of type {@code <T>}.
   */
  abstract void doWrite(Iterator<T> entries) throws IOException;

  /**
   * Advances the sequence id. If the sequence id exceeded the max limit, the timestamp will get updated and the
   * sequence id will get reset to 0.
   */
  private void incrementSequenceId() {
    seqId++;
    if (seqId >= SEQUENCE_ID_LIMIT) {
      updateTimeSequence();
    }
  }

  /**
   * Acquires a new timestamp and optionally reset the sequence id if the new timestamp is different than the last
   * used one.
   */
  private void updateTimeSequence() {
    writeTimestamp = timeProvider.currentTimeMillis();
    if (writeTimestamp == lastWriteTimestamp && seqId >= SEQUENCE_ID_LIMIT) {
      // Force the writeTimestamp to advance if we used up all sequence id.
      Uninterruptibles.sleepUninterruptibly(1L, TimeUnit.MILLISECONDS);
      writeTimestamp = timeProvider.currentTimeMillis();
    }

    if (writeTimestamp != lastWriteTimestamp) {
      lastWriteTimestamp = writeTimestamp;
      seqId = 0;
    }
  }

  /**
   * A resettable {@link Iterator} to transform payloads in a {@link PendingStoreRequest} to entries using
   * the {@link #getEntry(TopicId, boolean, long, long, short, byte[])} method.
   */
  private final class PayloadTransformIterator implements Iterator<T> {

    private final boolean generateNullPayloadEntry;
    private PendingStoreRequest storeRequest;
    private boolean computedFirst;
    private T nextEntry;
    private boolean completed = true;   // Initially, it is an empty iterator

    private PayloadTransformIterator(boolean generateNullPayloadEntry) {
      this.generateNullPayloadEntry = generateNullPayloadEntry;
    }

    @Override
    public boolean hasNext() {
      if (completed) {
        return false;
      }
      if (nextEntry != null) {
        return true;
      }

      // If the request has next payload
      // or if the iterator is empty but we wanted to generate an entry with null payload
      if (storeRequest.hasNext() || (generateNullPayloadEntry && !computedFirst)) {
        byte[] payload = storeRequest.hasNext() ? storeRequest.next() : null;
        nextEntry = getEntry(storeRequest.getTopicId(), storeRequest.isTransactional(),
                             storeRequest.getTransactionWritePointer(),
                             writeTimestamp, (short) seqId, payload);
      }
      computedFirst = true;
      completed = nextEntry == null;
      return !completed;
    }

    @Override
    public T next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      T next = nextEntry;
      storeRequest.setEndTimestamp(writeTimestamp);
      storeRequest.setEndSequenceId(seqId);
      incrementSequenceId();
      nextEntry = null;
      return next;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Delete not supported");
    }

    private PayloadTransformIterator reset(PendingStoreRequest storeRequest) {
      this.storeRequest = storeRequest;
      this.storeRequest.setStartTimestamp(writeTimestamp);
      this.storeRequest.setStartSequenceId(seqId);
      this.nextEntry = null;
      this.computedFirst = false;
      this.completed = false;
      return this;
    }
  }
}
