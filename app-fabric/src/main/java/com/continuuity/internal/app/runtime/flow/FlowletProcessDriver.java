package com.continuuity.internal.app.runtime.flow;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.flow.flowlet.Callback;
import com.continuuity.api.flow.flowlet.FailurePolicy;
import com.continuuity.api.flow.flowlet.FailureReason;
import com.continuuity.api.flow.flowlet.Flowlet;
import com.continuuity.api.flow.flowlet.InputContext;
import com.continuuity.app.queue.InputDatum;
import com.continuuity.common.logging.LoggingContext;
import com.continuuity.common.logging.LoggingContextAccessor;
import com.continuuity.internal.app.queue.SingleItemQueueReader;
import com.continuuity.internal.app.runtime.PostProcess;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class responsible invoking process methods one by one and commit the post process transaction.
 */
final class FlowletProcessDriver extends AbstractExecutionThreadService {

  private static final Logger LOG = LoggerFactory.getLogger(FlowletProcessDriver.class);
  private static final int TX_EXECUTOR_POOL_SIZE = 4;

  // Minimum back-off time in nanoseconds, 1ms.
  private static final long BACKOFF_MIN = TimeUnit.MILLISECONDS.toNanos(1);

  // Maximum back-off time in nanoseconds when increasing exponentially, 100ms.
  private static final long BACKOFF_MAX = TimeUnit.MILLISECONDS.toNanos(100);

  // Start time for switching from constant to exponentially increasing back-off time, 20ms.
  private static final long BACKOFF_EXP_START = TimeUnit.MILLISECONDS.toNanos(20);

  // Incrementing back-off time by this until reaching exponential increase range.
  private static final long BACKOFF_CONSTANT_INCREMENT = TimeUnit.MILLISECONDS.toNanos(1);

  // Doubling back-off time during exponential increase, up to maximum back-off time.
  private static final int BACKOFF_EXP = 2;

  private static final int PROCESS_MAX_RETRY = 10;

  private final Flowlet flowlet;
  private final BasicFlowletContext flowletContext;
  private final LoggingContext loggingContext;
  private final Collection<ProcessSpecification> processSpecs;
  private final Callback txCallback;
  private final AtomicReference<CountDownLatch> suspension;
  private final CyclicBarrier suspendBarrier;
  private final AtomicInteger inflight;
  private ExecutorService transactionExecutor;
  private Thread runnerThread;

  FlowletProcessDriver(Flowlet flowlet, BasicFlowletContext flowletContext,
                       Collection<ProcessSpecification> processSpecs,
                       Callback txCallback) {
    this.flowlet = flowlet;
    this.flowletContext = flowletContext;
    this.loggingContext = flowletContext.getLoggingContext();
    this.processSpecs = processSpecs;
    this.txCallback = txCallback;
    inflight = new AtomicInteger(0);

    this.suspension = new AtomicReference<CountDownLatch>();
    this.suspendBarrier = new CyclicBarrier(2);
  }

  @Override
  protected void startUp() throws Exception {
    if (flowletContext.isAsyncMode()) {
      ThreadFactory threadFactory = new ThreadFactory() {
        private final ThreadGroup threadGroup = new ThreadGroup("tx-thread");
        private final AtomicLong count = new AtomicLong(0);

        @Override
        public Thread newThread(Runnable r) {
          Thread t = new Thread(threadGroup, r, String.format("tx-executor-%d", count.getAndIncrement()));
          t.setDaemon(true);
          return t;
        }
      };

      // Thread pool of size max TX_EXECUTOR_POOL_SIZE.
      // 60 seconds wait time before killing idle threads.
      // Keep no idle threads more than 60 seconds.
      // If max thread pool size reached, execute the task in the submitter thread
      // (basically fallback to sync mode if things comes too fast
      transactionExecutor = new ThreadPoolExecutor(0, TX_EXECUTOR_POOL_SIZE,
                                    60L, TimeUnit.SECONDS,
                                    new SynchronousQueue<Runnable>(),
                                    threadFactory, new ThreadPoolExecutor.CallerRunsPolicy());
    } else {
      transactionExecutor = MoreExecutors.sameThreadExecutor();
    }
    runnerThread = Thread.currentThread();
    flowletContext.getSystemMetrics().gauge("process.instance", 1);
  }

  @Override
  protected void shutDown() throws Exception {
    try {
      transactionExecutor.shutdown();
      if (!transactionExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
        LOG.error("The transaction executor took more than 10 seconds to shutdown: " + flowletContext);
      }
    } finally {
      flowletContext.close();
    }
  }

  @Override
  protected void triggerShutdown() {
    runnerThread.interrupt();
  }

  /**
   * Suspend the running of flowlet. This method will block until the flowlet running thread actually suspended.
   */
  public void suspend() {
    if (suspension.compareAndSet(null, new CountDownLatch(1))) {
      try {
        suspendBarrier.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (BrokenBarrierException e) {
        LOG.error("Exception during suspend: " + flowletContext, e);
      }
    }
  }

  /**
   * Resume the running of flowlet.
   */
  public void resume() {
    CountDownLatch latch = suspension.getAndSet(null);
    if (latch != null) {
      suspendBarrier.reset();
      latch.countDown();
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  protected void run() {
    LoggingContextAccessor.setLoggingContext(loggingContext);

    initFlowlet();

    // Insert all into priority queue, ordered by next deque time.
    PriorityBlockingQueue<ProcessEntry<?>> processQueue =
      new PriorityBlockingQueue<ProcessEntry<?>>(processSpecs.size());
    for (ProcessSpecification<?> spec : processSpecs) {
      processQueue.offer(ProcessEntry.create(spec));
    }
    List<ProcessEntry<?>> processList = Lists.newArrayListWithExpectedSize(processSpecs.size() * 2);

    while (isRunning()) {
      CountDownLatch suspendLatch = suspension.get();
      if (suspendLatch != null) {
        try {
          waitForInflight(processQueue);
          suspendBarrier.await();
          suspendLatch.await();
        } catch (Exception e) {
          // Simply continue and let the isRunning() check to deal with that.
          continue;
        }
      }

      try {
        // If the queue head need to wait, we had to wait.
        processQueue.peek().await();
      } catch (InterruptedException e) {
        // Triggered by shutdown, simply continue and let the isRunning() check to deal with that.
        continue;
      }

      processList.clear();
      processQueue.drainTo(processList);

      for (ProcessEntry<?> entry : processList) {
        boolean invoked = false;
        try {
          if (!entry.shouldProcess()) {
            continue;
          }

          ProcessMethod processMethod = entry.getProcessSpec().getProcessMethod();

          if (processMethod.needsInput()) {
            flowletContext.getSystemMetrics().gauge("process.tuples.attempt.read", 1);
          }
          InputDatum input = entry.getProcessSpec().getQueueReader().dequeue();
          if (!input.needProcess()) {
            entry.backOff();
            continue;
          }

          // Resetting back-off time to minimum back-off time, since an entry to process was de-queued and most likely
          // more entries will follow.
          entry.resetBackOff();

          if (!entry.isRetry()) {
            // Only increment the inflight count for non-retry entries.
            // The inflight count would get decrement when the transaction committed successfully or input get ignored.
            // See the processMethodCallback function.
            inflight.getAndIncrement();
          }

          try {
            invoked = true;
            // Call the process method and commit the transaction
            processMethod.invoke(input, wrapInputDatumDecoder(input, entry.getProcessSpec().getInputDatumDecoder()))
              .commit(transactionExecutor, processMethodCallback(processQueue, entry, input));

          } catch (Throwable t) {
            LOG.error("Fail to invoke process method: {}, {}", entry.getProcessSpec(), flowletContext, t);
          }
        } catch (OperationException e) {
          LOG.error("Queue operation failure: " + flowletContext, e);
        } finally {
          // In async mode, always put it back as long as it is not a retry entry,
          // otherwise let the committer do the job.
          if (!invoked || (flowletContext.isAsyncMode() && !entry.isRetry())) {
            processQueue.offer(entry);
          }
        }
      }

    }
    waitForInflight(processQueue);

    destroyFlowlet();
  }

  private <T> Function<ByteBuffer, T> wrapInputDatumDecoder(final InputDatum input,
                                                            final Function<ByteBuffer, T> inputDatumDecoder) {
    return new Function<ByteBuffer, T>() {
      @Override
      public T apply(ByteBuffer byteBuffer) {
        flowletContext.getSystemMetrics().gauge("process.events.ins." + input.getInputContext().getOrigin(), 1);
        flowletContext.getSystemMetrics().gauge("process.tuples.read", 1);
        return inputDatumDecoder.apply(byteBuffer);
      }
    };
  }

  /**
   * Wait for all inflight processes in the queue.
   * @param processQueue list of inflight processes
   */
  @SuppressWarnings("unchecked")
  private void waitForInflight(PriorityBlockingQueue<ProcessEntry<?>> processQueue) {
    List<ProcessEntry> processList = Lists.newArrayListWithCapacity(processQueue.size());
    boolean hasRetry;

    do {
      hasRetry = false;
      processList.clear();
      processQueue.drainTo(processList);

      for (ProcessEntry<?> entry : processList) {
        if (!entry.isRetry()) {
          processQueue.offer(entry);
          continue;
        }
        hasRetry = true;
        try {
          ProcessMethod processMethod = entry.getProcessSpec().getProcessMethod();
          InputDatum input = entry.getProcessSpec().getQueueReader().dequeue();
          flowletContext.getSystemMetrics().gauge("process.tuples.attempt.read", 1);

          try {
            // Call the process method and commit the transaction
            processMethod.invoke(input, wrapInputDatumDecoder(input, entry.getProcessSpec().getInputDatumDecoder()))
              .commit(transactionExecutor, processMethodCallback(processQueue, entry, input));

          } catch (Throwable t) {
            LOG.error("Fail to invoke process method: {}, {}", entry.getProcessSpec(), flowletContext, t);
          }
        } catch (OperationException e) {
          // This should never happen for retry entries
          LOG.error("Queue operation failure: " + flowletContext, e);
        }
      }
    } while (hasRetry || inflight.get() != 0);
  }

  private void initFlowlet() {
    try {
      LOG.info("Initializing flowlet: " + flowletContext);
      flowlet.initialize(flowletContext);
      LOG.info("Flowlet initialized: " + flowletContext);
    } catch (Throwable t) {
      LOG.error("Flowlet throws exception during flowlet initialize: " + flowletContext, t);
      throw Throwables.propagate(t);
    }
  }

  private void destroyFlowlet() {
    try {
      LOG.info("Destroying flowlet: " + flowletContext);
      flowlet.destroy();
      LOG.info("Flowlet destroyed: " + flowletContext);
    } catch (Throwable t) {
      LOG.error("Flowlet throws exception during flowlet destroy: " + flowletContext, t);
    }
  }

  private <T> PostProcess.Callback processMethodCallback(final PriorityBlockingQueue<ProcessEntry<?>> processQueue,
                                                     final ProcessEntry<T> processEntry,
                                                     final InputDatum input) {
    return new PostProcess.Callback() {
      @Override
      public void onSuccess(Object object, InputContext inputContext) {
        try {
          flowletContext.getSystemMetrics().gauge("process.events.processed", 1);
          txCallback.onSuccess(object, inputContext);
        } catch (Throwable t) {
          LOG.info("Exception on onSuccess call: " + flowletContext, t);
        } finally {
          enqueueEntry();
          inflight.decrementAndGet();
        }
      }

      @Override
      public void onFailure(Object inputObject, InputContext inputContext, FailureReason reason,
                            PostProcess.InputAcknowledger inputAcknowledger) {

        LOG.info("Process failure. " + reason.getMessage() + ", input: " + input, reason.getCause().getCause());
        FailurePolicy failurePolicy;
        try {
          flowletContext.getSystemMetrics().gauge("process.errors", 1);
          failurePolicy = txCallback.onFailure(inputObject, inputContext, reason);
        } catch (Throwable t) {
          LOG.error("Exception on onFailure call: " + flowletContext, t);
          failurePolicy = FailurePolicy.RETRY;
        }

        if (input.getRetry() >= PROCESS_MAX_RETRY) {
          LOG.info("Too many retries, ignoring the input: " + input);
          failurePolicy = FailurePolicy.IGNORE;
        }

        if (failurePolicy == FailurePolicy.RETRY) {
          ProcessEntry retryEntry = processEntry.isRetry() ?
            processEntry :
            new ProcessEntry<T>(processEntry.getProcessSpec(),
              new ProcessSpecification<T>(new SingleItemQueueReader(input),
                                       processEntry.getProcessSpec().getInputDatumDecoder(),
                                       processEntry.getProcessSpec().getProcessMethod()));

          processQueue.offer(retryEntry);

        } else if (failurePolicy == FailurePolicy.IGNORE) {
          try {
            flowletContext.getSystemMetrics().gauge("process.events.processed", 1);
            inputAcknowledger.ack();
          } catch (OperationException e) {
            LOG.error("Fatal problem, fail to ack an input: " + flowletContext, e);
          } finally {
            enqueueEntry();
            inflight.decrementAndGet();
          }
        }
      }

      private void enqueueEntry() {
        if (!flowletContext.isAsyncMode()) {
          processQueue.offer(processEntry.resetRetry());
        }
      }
    };
  }

  private static final class ProcessEntry<T> implements Comparable<ProcessEntry> {
    private final ProcessSpecification<T> processSpec;
    private final ProcessSpecification<T> retrySpec;
    private long nextDeque;
    private long currentBackOff = BACKOFF_MIN;

    private static <T> ProcessEntry<T> create(ProcessSpecification<T> processSpec) {
      return new ProcessEntry<T>(processSpec);
    }

    private ProcessEntry(ProcessSpecification<T> processSpec) {
      this(processSpec, null);
    }

    private ProcessEntry(ProcessSpecification<T> processSpec, ProcessSpecification<T> retrySpec) {
      this.processSpec = processSpec;
      this.retrySpec = retrySpec;
    }

    public boolean isRetry() {
      return retrySpec != null;
    }

    public void await() throws InterruptedException {
      long waitTime = nextDeque - System.nanoTime();
      if (waitTime > 0) {
        TimeUnit.NANOSECONDS.sleep(waitTime);
      }
    }

    public boolean shouldProcess() {
      return nextDeque - System.nanoTime() <= 0;
    }

    @Override
    public int compareTo(ProcessEntry o) {
      if (nextDeque == o.nextDeque) {
        return 0;
      }
      return nextDeque > o.nextDeque ? 1 : -1;
    }

    public void resetBackOff() {
      nextDeque = 0;
      currentBackOff = BACKOFF_MIN;
    }

    public void backOff() {
      nextDeque = System.nanoTime() + currentBackOff;
      if (currentBackOff < BACKOFF_EXP_START) {
        currentBackOff += BACKOFF_CONSTANT_INCREMENT;
      } else {
        currentBackOff = Math.min(currentBackOff * BACKOFF_EXP, BACKOFF_MAX);
      }
    }

    public ProcessSpecification<T> getProcessSpec() {
      return retrySpec == null ? processSpec : retrySpec;
    }

    public ProcessEntry<T> resetRetry() {
      return retrySpec == null ? this : new ProcessEntry<T>(processSpec);
    }
  }
}
