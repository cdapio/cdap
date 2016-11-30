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

import co.cask.cdap.api.metrics.MetricsCollector;
import co.cask.cdap.api.metrics.NoopMetricsContext;
import co.cask.cdap.messaging.RollbackDetail;
import co.cask.cdap.messaging.StoreRequest;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Class to support writing to message/payload tables with high concurrency.
 *
 * It uses a non-blocking algorithm to batch writes from concurrent threads. The algorithm is the same as
 * the one used in ConcurrentStreamWriter.
 *
 * The algorithm is like this:
 *
 * When a thread that received a request, performs the following:
 *
 * <pre>
 * 1. Constructs a PendingStoreRequest locally and enqueue it to a ConcurrentLinkedQueue.
 * 2. Use CAS to set an AtomicBoolean flag to true.
 * 3. If successfully set the flag to true, this thread becomes the writer and proceed to run step 4-7.
 * 4. Provides an Iterator of PendingStoreRequest, which consumes from the ConcurrentLinkedQueue mentioned in step 1.
 * 5. The message table store method will consume the Iterator until it is empty
 * 6. Set the state of each PendingStoreRequest that are written to COMPLETED (succeed/failure).
 * 7. Set the AtomicBoolean flag back to false.
 * 8. If the PendingStoreRequest enqueued by this thread is NOT COMPLETED, go back to step 2.
 * </pre>
 *
 * The spin lock between step 2 to step 8 is necessary as it guarantees events enqueued by all threads would eventually
 * get written and flushed.
 */
@ThreadSafe
final class ConcurrentMessageWriter implements Closeable {

  private final StoreRequestWriter<?> messagesWriter;
  private final MetricsCollector metricsCollector;
  private final PendingStoreQueue pendingStoreQueue;
  private final AtomicBoolean writerFlag;
  private final AtomicBoolean closed;

  /**
   * Constructor with a {@link NoopMetricsContext}. This constructor should only be used in unit-testing.
   */
  @VisibleForTesting
  ConcurrentMessageWriter(StoreRequestWriter<?> messagesWriter) {
    this(messagesWriter, new NoopMetricsContext());
  }

  /**
   * Constructor.
   *
   * @param messagesWriter the {@link StoreRequestWriter} for persisting {@link StoreRequest}.
   * @param metricsCollector the {@link MetricsCollector} for collecting metrics emitted by this class.
   */
  ConcurrentMessageWriter(StoreRequestWriter<?> messagesWriter, MetricsCollector metricsCollector) {
    this.messagesWriter = messagesWriter;
    this.metricsCollector = metricsCollector;
    this.pendingStoreQueue = new PendingStoreQueue(metricsCollector);
    this.writerFlag = new AtomicBoolean();
    this.closed = new AtomicBoolean();
  }

  /**
   * Persists the given {@link StoreRequest} to the {@link StoreRequestWriter} in this class. This method
   * is safe to be called concurrently from multiple threads.
   *
   * @param storeRequest contains information about payload to be store
   * @return if the store request is transactional, then returns a {@link RollbackDetail} containing
   *         information for rollback; otherwise {@code null} will be returned.
   * @throws IOException if failed to persist the data
   */
  @Nullable
  RollbackDetail persist(StoreRequest storeRequest) throws IOException {
    if (closed.get()) {
      throw new IOException("Message writer is already closed");
    }

    PendingStoreRequest pendingStoreRequest = new PendingStoreRequest(storeRequest);
    pendingStoreQueue.enqueue(pendingStoreRequest);

    metricsCollector.increment("persist.requested", 1L);

    while (!pendingStoreRequest.isCompleted()) {
      if (!tryWrite()) {
        Thread.yield();
      }
    }

    if (pendingStoreRequest.isSuccess()) {
      metricsCollector.increment("persist.success", 1L);
      if (!pendingStoreRequest.isTransactional()) {
        return null;
      }
      return new SimpleRollbackDetail(pendingStoreRequest.getTransactionWritePointer(),
                                      pendingStoreRequest.getStartTimestamp(), pendingStoreRequest.getStartSequenceId(),
                                      pendingStoreRequest.getEndTimestamp(), pendingStoreRequest.getEndSequenceId());
    } else {
      metricsCollector.increment("persist.failure", 1L);
      Throwables.propagateIfInstanceOf(pendingStoreRequest.getFailureCause(), IOException.class);
      throw new IOException("Unable to write message to " + storeRequest.getTopicId(),
                            pendingStoreRequest.getFailureCause());
    }
  }

  /**
   * Tries to acquire the writer flag and persist the pending requests.
   *
   * @return {@code true} if acquired the writer flag and called {@link PendingStoreQueue#persist(StoreRequestWriter)};
   *         otherwise {@code false} will be returned.
   */
  private boolean tryWrite() {
    if (!writerFlag.compareAndSet(false, true)) {
      return false;
    }
    try {
      pendingStoreQueue.persist(messagesWriter);
    } finally {
      writerFlag.set(false);
    }
    return true;
  }

  @Override
  public void close() throws IOException {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    // Flush everything in the queue.
    // When this thread can grab the writer flag, all pending write requests must be completed since the closed
    // flag was already set to false.
    while (!tryWrite()) {
      Thread.yield();
    }
    messagesWriter.close();
  }

  /**
   * A resettable {@link Iterator} to provide {@link StoreRequest} to {@link StoreRequestWriter}.
   * Except the {@link #enqueue(PendingStoreRequest)} method, all methods on this class can only be
   * called while holding the writer flag.
   */
  private static final class PendingStoreQueue {

    private final MetricsCollector metricsCollector;
    private final Queue<PendingStoreRequest> writeQueue;
    private final List<PendingStoreRequest> inflightRequests;

    private PendingStoreQueue(MetricsCollector metricsCollector) {
      this.metricsCollector = metricsCollector;
      this.writeQueue = new ConcurrentLinkedQueue<>();
      this.inflightRequests = new ArrayList<>(100);
    }

    /**
     * Puts the given {@link PendingStoreRequest} to the concurrent queue.
     */
    void enqueue(PendingStoreRequest storeRequest) {
      writeQueue.add(storeRequest);
    }

    /**
     * Persists all {@link PendingStoreRequest} currently in the queue with the given writer.
     */
    void persist(StoreRequestWriter<?> writer) {
      // Capture all current events.
      // The reason for capturing instead of using a live iterator is to avoid the possible case of infinite write
      // time. E.g. while generating the entry to write to the storage table, a new store request get enqueued.
      // The number of requests in the queue is bounded by the number of threads that call this method.
      // Since this method is expected to be called (indirectly) from a http handler thread, that is bounded by
      // the thread pool size used by the http service.
      inflightRequests.clear();
      PendingStoreRequest request = writeQueue.poll();
      while (request != null) {
        inflightRequests.add(request);
        request = writeQueue.poll();
      }

      metricsCollector.gauge("persist.queue.size", inflightRequests.size());

      try {
        writer.write(inflightRequests.iterator());
        completeAll(null);
      } catch (Throwable t) {
        completeAll(t);
      }
    }

    /**
     * Marks all inflight requests as collected through the {@link Iterator#next()} method as completed.
     * This method must be called while holding the writer flag.
     */
    void completeAll(@Nullable Throwable failureCause) {
      Iterator<PendingStoreRequest> iterator = inflightRequests.iterator();
      while (iterator.hasNext()) {
        iterator.next().completed(failureCause);
        iterator.remove();
      }
    }
  }

  /**
   * Straightforward implementation of {@link RollbackDetail}
   */
  private static final class SimpleRollbackDetail implements RollbackDetail {

    private final long transactionWritePointer;
    private final long startTimestamp;
    private final int startSequenceId;
    private final long endTimestamp;
    private final int endSequenceId;

    SimpleRollbackDetail(long transactionWritePointer, long startTimestamp,
                         int startSequenceId, long endTimestamp, int endSequenceId) {
      this.transactionWritePointer = transactionWritePointer;
      this.startTimestamp = startTimestamp;
      this.startSequenceId = startSequenceId;
      this.endTimestamp = endTimestamp;
      this.endSequenceId = endSequenceId;
    }

    @Override
    public long getTransactionWritePointer() {
      return transactionWritePointer;
    }

    @Override
    public long getStartTimestamp() {
      return startTimestamp;
    }

    @Override
    public int getStartSequenceId() {
      return startSequenceId;
    }

    @Override
    public long getEndTimestamp() {
      return endTimestamp;
    }

    @Override
    public int getEndSequenceId() {
      return endSequenceId;
    }
  }
}
