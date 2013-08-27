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
import com.continuuity.data.operation.executor.TransactionAgent;
import com.continuuity.internal.app.queue.SingleItemQueueReader;
import com.continuuity.internal.app.runtime.DataFabricFacade;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
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

  private final Flowlet flowlet;
  private final BasicFlowletContext flowletContext;
  private final LoggingContext loggingContext;
  private final Collection<ProcessSpecification> processSpecs;
  private final Callback txCallback;
  private final AtomicReference<CountDownLatch> suspension;
  private final CyclicBarrier suspendBarrier;
  private final AtomicInteger inflight;
  private final DataFabricFacade dataFabricFacade;
  private ExecutorService transactionExecutor;
  private Thread runnerThread;

  FlowletProcessDriver(Flowlet flowlet, BasicFlowletContext flowletContext,
                       Collection<ProcessSpecification> processSpecs,
                       Callback txCallback, DataFabricFacade dataFabricFacade) {
    this.flowlet = flowlet;
    this.flowletContext = flowletContext;
    this.loggingContext = flowletContext.getLoggingContext();
    this.processSpecs = processSpecs;
    this.txCallback = txCallback;
    this.dataFabricFacade = dataFabricFacade;
    this.inflight = new AtomicInteger(0);

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

  @Override
  protected String getServiceName() {
    return getClass().getSimpleName() + "-" + flowletContext.getName() + "-" + flowletContext.getInstanceId();
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
    BlockingQueue<FlowletProcessEntry<?>> processQueue =
      new PriorityBlockingQueue<FlowletProcessEntry<?>>(processSpecs.size());
    for (ProcessSpecification<?> spec : processSpecs) {
      processQueue.offer(FlowletProcessEntry.create(spec));
    }
    List<FlowletProcessEntry<?>> processList = Lists.newArrayListWithExpectedSize(processSpecs.size() * 2);

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

      for (FlowletProcessEntry<?> entry : processList) {
        boolean invoked = false;
        try {
          if (!entry.shouldProcess()) {
            continue;
          }

          ProcessMethod processMethod = entry.getProcessSpec().getProcessMethod();
          if (processMethod.needsInput()) {
            flowletContext.getSystemMetrics().gauge("process.tuples.attempt.read", 1);
          }

          // Begin transaction and dequeue
          TransactionAgent txAgent = dataFabricFacade.createAndUpdateTransactionAgentProxy();
          try {
            txAgent.start();

            InputDatum input = entry.getProcessSpec().getQueueReader().dequeue();
            if (!input.needProcess()) {
              entry.backOff();
              // End the transaction if nothing in the queue
              txAgent.finish();
              continue;
            }

            // Resetting back-off time to minimum back-off time, since an entry to process was de-queued and most likely
            // more entries will follow.
            entry.resetBackOff();

            if (!entry.isRetry()) {
              // Only increment the inflight count for non-retry entries.
              // The inflight would get decrement when the transaction committed successfully or input get ignored.
              // See the processMethodCallback function.
              inflight.getAndIncrement();
            }

            try {
              // Call the process method and commit the transaction
              ProcessMethod.ProcessResult result =
                processMethod.invoke(input, wrapInputDecoder(input, entry.getProcessSpec().getInputDecoder()));
              postProcess(transactionExecutor, processMethodCallback(processQueue, entry, input), txAgent, input,
                          result);
            } finally {
              invoked = true;
            }

          } catch (Throwable t) {
            LOG.error("Unexpected exception: {}", flowletContext, t);
            try {
              txAgent.abort();
            } catch (OperationException e) {
              LOG.error("Fail to abort transaction: {}", flowletContext, e);
            }
          }
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

  private <T> Function<ByteBuffer, T> wrapInputDecoder(final InputDatum input,
                                                       final Function<ByteBuffer, T> inputDecoder) {
    final String eventsMetricsName = "process.events.ins." + input.getInputContext().getOrigin();
    return new Function<ByteBuffer, T>() {
      @Override
      public T apply(ByteBuffer byteBuffer) {
        flowletContext.getSystemMetrics().gauge(eventsMetricsName, 1);
        flowletContext.getSystemMetrics().gauge("process.tuples.read", 1);
        return inputDecoder.apply(byteBuffer);
      }
    };
  }

  private void postProcess(Executor executor, final ProcessMethodCallback callback, final TransactionAgent txAgent,
                           final InputDatum input, final ProcessMethod.ProcessResult result) {
    executor.execute(new Runnable() {
      @Override
      public void run() {
        InputContext inputContext = input.getInputContext();
        Throwable failureCause = null;
        try {
          if (result.isSuccess()) {
            // If it is a retry input, force the dequeued entries into current transaction.
            if (input.getRetry() > 0) {
              input.reclaim();
            }
            txAgent.finish();
          } else {
            failureCause = result.getCause();
            txAgent.abort();
          }
        } catch (OperationException e) {
          LOG.error("Transaction operation failed: {}", e.getMessage(), e);
          failureCause = e;
          try {
            if (result.isSuccess()) {
              txAgent.abort();
            }
          } catch (OperationException ex) {
            LOG.error("Fail to abort transaction: {}", inputContext, ex);
          }
        } finally {
          // we want to emit metrics after every retry after finish() so that deferred operations are also logged
          flowletContext.getSystemMetrics().gauge("store.ops", txAgent.getSucceededCount());
        }

        if (failureCause == null) {
          callback.onSuccess(result.getEvent(), inputContext);
        } else {
          callback.onFailure(result.getEvent(), inputContext,
                             new FailureReason(FailureReason.Type.USER, failureCause.getMessage(), failureCause),
                             createInputAcknowledger(input));
        }
      }
    });
  }

  private InputAcknowledger createInputAcknowledger(final InputDatum input) {
    return new InputAcknowledger() {
      @Override
      public void ack() throws OperationException {
        TransactionAgent txAgent = dataFabricFacade.createTransactionAgent();
        txAgent.start();
        input.reclaim();
        txAgent.finish();
      }
    };
  }

  /**
   * Wait for all inflight processes in the queue.
   * @param processQueue list of inflight processes
   */
  @SuppressWarnings("unchecked")
  private void waitForInflight(BlockingQueue<FlowletProcessEntry<?>> processQueue) {
    List<FlowletProcessEntry> processList = Lists.newArrayListWithCapacity(processQueue.size());
    boolean hasRetry;

    do {
      hasRetry = false;
      processList.clear();
      processQueue.drainTo(processList);

      for (FlowletProcessEntry<?> entry : processList) {
        if (!entry.isRetry()) {
          processQueue.offer(entry);
          continue;
        }
        hasRetry = true;
        ProcessMethod processMethod = entry.getProcessSpec().getProcessMethod();

        TransactionAgent txAgent = dataFabricFacade.createAndUpdateTransactionAgentProxy();
        try {
          txAgent.start();
          InputDatum input = entry.getProcessSpec().getQueueReader().dequeue();
          flowletContext.getSystemMetrics().gauge("process.tuples.attempt.read", input.size());

          // Call the process method and commit the transaction
          ProcessMethod.ProcessResult result =
            processMethod.invoke(input, wrapInputDecoder(input, entry.getProcessSpec().getInputDecoder()));
          postProcess(transactionExecutor, processMethodCallback(processQueue, entry, input), txAgent, input, result);

        } catch (Throwable t) {
          LOG.error("Unexpected exception: {}", flowletContext, t);
          try {
            txAgent.abort();
          } catch (OperationException e) {
            LOG.error("Fail to abort transaction: {}", flowletContext, e);
          }
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

  private <T> ProcessMethodCallback processMethodCallback(final BlockingQueue<FlowletProcessEntry<?>> processQueue,
                                                          final FlowletProcessEntry<T> processEntry,
                                                          final InputDatum input) {
    return new ProcessMethodCallback() {
      @Override
      public void onSuccess(Object object, InputContext inputContext) {
        try {
          flowletContext.getSystemMetrics().gauge("process.events.processed", input.size());
          txCallback.onSuccess(object, inputContext);
        } catch (Throwable t) {
          LOG.error("Exception on onSuccess call: {}", flowletContext, t);
        } finally {
          enqueueEntry();
          inflight.decrementAndGet();
        }
      }

      @Override
      public void onFailure(Object inputObject, InputContext inputContext, FailureReason reason,
                            InputAcknowledger inputAcknowledger) {

        LOG.warn("Process failure: {}, {}, input: {}", flowletContext, reason.getMessage(), input, reason.getCause());
        FailurePolicy failurePolicy;
        try {
          flowletContext.getSystemMetrics().gauge("process.errors", 1);
          failurePolicy = txCallback.onFailure(inputObject, inputContext, reason);
          if (failurePolicy == null) {
            failurePolicy = FailurePolicy.RETRY;
            LOG.info("Callback returns null for failure policy. Default to {}.", failurePolicy);
          }
        } catch (Throwable t) {
          LOG.error("Exception on onFailure call: {}", flowletContext, t);
          failurePolicy = FailurePolicy.RETRY;
        }

        if (input.getRetry() >= processEntry.getProcessSpec().getProcessMethod().getMaxRetries()) {
          LOG.info("Too many retries, ignores the input: {}", input);
          failurePolicy = FailurePolicy.IGNORE;
        }

        if (failurePolicy == FailurePolicy.RETRY) {
          FlowletProcessEntry retryEntry = processEntry.isRetry() ?
            processEntry :
            FlowletProcessEntry.create(processEntry.getProcessSpec(),
                                       new ProcessSpecification<T>(new SingleItemQueueReader(input),
                                                                   processEntry.getProcessSpec().getInputDecoder(),
                                                                   processEntry.getProcessSpec().getProcessMethod()));
          processQueue.offer(retryEntry);

        } else if (failurePolicy == FailurePolicy.IGNORE) {
          try {
            flowletContext.getSystemMetrics().gauge("process.events.processed", input.size());
            inputAcknowledger.ack();
          } catch (OperationException e) {
            LOG.error("Fatal problem, fail to ack an input: {}", flowletContext, e);
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
}
