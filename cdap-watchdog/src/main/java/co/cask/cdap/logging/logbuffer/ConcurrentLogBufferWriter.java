/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.logging.logbuffer;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.logging.pipeline.logbuffer.LogBufferProcessorPipeline;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;

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
 * Class to support writing to log buffer and log processing pipeline(s) with high concurrency.
 *
 * It uses a non-blocking algorithm to batch writes from concurrent threads. The algorithm is the same as
 * the one used in ConcurrentMessageWriter.
 *
 * The algorithm is like this:
 *
 * When a thread that received a request, performs the following:
 *
 * <pre>
 * 1. Constructs a PendingLogBufferRequest locally and enqueue it to a ConcurrentLinkedQueue.
 * 2. Use CAS to set an AtomicBoolean flag to true.
 * 3. If successfully set the flag to true, this thread becomes the writer and proceed to run step 4-7.
 * 4. Provides an Iterator of PendingLogBufferRequest to log buffer writer, which consumes from the
 * ConcurrentLinkedQueue.
 * 5. Log buffer writer returns list of log events with file offset. These events are sent to log processor pipeline
 * for further processing.
 * 6. Set the state of each PendingLogBufferRequest that are written to COMPLETED (succeed/failure).
 * 7. Set the AtomicBoolean flag back to false.
 * 8. If the PendingLogBufferRequest enqueued by this thread is NOT COMPLETED, go back to step 2.
 * </pre>
 *
 * TODO: CDAP-14937 Restore unprocessed log events from LogBuffer(WAL).
 */
@ThreadSafe
public class ConcurrentLogBufferWriter implements Closeable {
  // concurrent queue to write pending log buffer requests
  private final PendingRequestQueue pendingRequestQueue;
  // WAL writer to buffer log events
  private final LogBufferWriter logBufferWriter;
  private final List<LogBufferProcessorPipeline> pipelines;
  private final AtomicBoolean writerFlag;
  private final AtomicBoolean closed;

  public ConcurrentLogBufferWriter(CConfiguration cConf,
                                   List<LogBufferProcessorPipeline> pipelines) throws IOException {
    this.pendingRequestQueue = new PendingRequestQueue();
    this.logBufferWriter = new LogBufferWriter(cConf.get(Constants.LogBuffer.LOG_BUFFER_BASE_DIR),
                                               cConf.getLong(Constants.LogBuffer.LOG_BUFFER_MAX_FILE_SIZE_BYTES));
    this.pipelines = pipelines;
    this.writerFlag = new AtomicBoolean();
    this.closed = new AtomicBoolean();
  }

  public void process(LogBufferRequest request) throws IOException {
    if (closed.get()) {
      throw new IOException("Concurrent log writer is already closed.");
    }

    PendingLogBufferRequest pendingLogBufferRequest = new PendingLogBufferRequest(request);
    pendingRequestQueue.enqueue(pendingLogBufferRequest);

    while (!pendingLogBufferRequest.isCompleted()) {
      if (!tryWrite()) {
        Thread.yield();
      }
    }

    if (!pendingLogBufferRequest.isSuccess()) {
      Throwables.propagateIfInstanceOf(pendingLogBufferRequest.getFailureCause(), IOException.class);
      throw new IOException("Unable to write log event to log buffer", pendingLogBufferRequest.getFailureCause());
    }
  }

  /**
   * Tries to acquire the writer flag and processes the pending requests.
   *
   * @return {@code true} if acquired the writer flag and called {@link PendingRequestQueue#process(LogBufferRequest)};
   *         otherwise {@code false} will be returned.
   */
  private boolean tryWrite() {
    if (!writerFlag.compareAndSet(false, true)) {
      return false;
    }
    try {
      pendingRequestQueue.process(logBufferWriter, pipelines);
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
    logBufferWriter.close();
  }

  /**
   * An in memory queue to hold concurrent log requests. Except the {@link #enqueue(PendingLogBufferRequest)} method,
   * all methods on this class can only be called while holding the writer flag.
   */
  private static final class PendingRequestQueue {

    private final Queue<PendingLogBufferRequest> writeQueue;
    private final List<PendingLogBufferRequest> inflightRequests;

    private PendingRequestQueue() {
      this.writeQueue = new ConcurrentLinkedQueue<>();
      this.inflightRequests = new ArrayList<>(100);
    }

    void enqueue(PendingLogBufferRequest bufferRequest) {
      writeQueue.add(bufferRequest);
    }

    void process(LogBufferWriter writer, List<LogBufferProcessorPipeline> pipelines) {
      // Capture all current events.
      // The reason for capturing instead of using a live iterator is to avoid the possible case of infinite write
      // time. The number of requests in the queue is bounded by the number of threads that call this method.
      // Since this method is expected to be called from a http handler thread, that is bounded by
      // the thread pool size used by the http service.
      inflightRequests.clear();
      PendingLogBufferRequest request = writeQueue.poll();
      while (request != null) {
        inflightRequests.add(request);
        request = writeQueue.poll();
      }

      try {
        if (!inflightRequests.isEmpty()) {
          // persist logs in append only WAL
          Iterable<LogBufferEvent> events = writer.write(new PendingEventIterator(inflightRequests.iterator()));
          for (LogBufferProcessorPipeline pipeline : pipelines) {
            pipeline.processLogEvents(events.iterator());
          }
        }
        // after log events have been sent to log buffer pipeline for processing, set the pending requests to complete
        completeAll(null);
      } catch (Throwable t) {
        // This will only happen if logs were not written to log buffer(WAL), so the log buffer request must be
        // retried. So complete all the requests with error.
        completeAll(t);
      }
    }

    /**
     * Marks all inflight requests as collected through the {@link Iterator#next()} method as completed.
     * This method must be called while holding the writer flag.
     */
    void completeAll(@Nullable Throwable failureCause) {
      Iterator<PendingLogBufferRequest> iterator = inflightRequests.iterator();
      while (iterator.hasNext()) {
        iterator.next().completed(failureCause);
        iterator.remove();
      }
    }

    /**
     * Pending log events iterator.
     */
    static final class PendingEventIterator extends AbstractIterator<byte[]> {
      Iterator<PendingLogBufferRequest> requestIterator;
      Iterator<byte[]> currentRequestIterator;

      PendingEventIterator(Iterator<PendingLogBufferRequest> requestIterator) {
        this.requestIterator = requestIterator;
        this.currentRequestIterator = requestIterator.next().getIterator();
      }

      @Override
      protected byte[] computeNext() {
        if (currentRequestIterator.hasNext()) {
          return currentRequestIterator.next();
        }

        while (!currentRequestIterator.hasNext()) {
          if (!requestIterator.hasNext()) {
            return endOfData();
          }
          currentRequestIterator = requestIterator.next().getIterator();
        }

        return currentRequestIterator.next();
      }
    }
  }
}
