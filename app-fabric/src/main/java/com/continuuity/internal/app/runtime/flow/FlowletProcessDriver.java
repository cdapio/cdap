package com.continuuity.internal.app.runtime.flow;

import com.continuuity.api.flow.flowlet.Callback;
import com.continuuity.api.flow.flowlet.FailurePolicy;
import com.continuuity.api.flow.flowlet.FailureReason;
import com.continuuity.api.flow.flowlet.Flowlet;
import com.continuuity.api.flow.flowlet.InputContext;
import com.continuuity.app.queue.InputDatum;
import com.continuuity.common.logging.LoggingContext;
import com.continuuity.common.logging.LoggingContextAccessor;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.transaction.TransactionContext;
import com.continuuity.data2.transaction.TransactionExecutor;
import com.continuuity.data2.transaction.TransactionFailureException;
import com.continuuity.internal.app.queue.SingleItemQueueReader;
import com.continuuity.internal.app.runtime.DataFabricFacade;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class responsible invoking process methods one by one and commit the post process transaction.
 */
final class FlowletProcessDriver extends AbstractExecutionThreadService {

  private static final Logger LOG = LoggerFactory.getLogger(FlowletProcessDriver.class);

  private final Flowlet flowlet;
  private final BasicFlowletContext flowletContext;
  private final LoggingContext loggingContext;
  private final Collection<ProcessSpecification> processSpecs;
  private final Callback txCallback;
  private final AtomicReference<CountDownLatch> suspension;
  private final CyclicBarrier suspendBarrier;
  private final AtomicInteger inflight;
  private final DataFabricFacade dataFabricFacade;
  private final Service serviceHook;

  private Thread runnerThread;
  private ExecutorService processExecutor;

  FlowletProcessDriver(Flowlet flowlet, BasicFlowletContext flowletContext,
                       Collection<ProcessSpecification> processSpecs,
                       Callback txCallback, DataFabricFacade dataFabricFacade,
                       Service serviceHook) {
    this.flowlet = flowlet;
    this.flowletContext = flowletContext;
    this.loggingContext = flowletContext.getLoggingContext();
    this.processSpecs = processSpecs;
    this.txCallback = txCallback;
    this.dataFabricFacade = dataFabricFacade;
    this.serviceHook = serviceHook;
    this.inflight = new AtomicInteger(0);

    this.suspension = new AtomicReference<CountDownLatch>();
    this.suspendBarrier = new CyclicBarrier(2);
  }

  @Override
  protected void startUp() throws Exception {
    runnerThread = Thread.currentThread();
    flowletContext.getSystemMetrics().gauge("process.instance", 1);
    processExecutor = Executors.newSingleThreadExecutor(
      Threads.createDaemonThreadFactory(getServiceName() + "-executor"));
  }

  @Override
  protected void shutDown() throws Exception {
    processExecutor.shutdown();
    flowletContext.close();
  }

  @Override
  protected void triggerShutdown() {
    LOG.info("Shutting down flowlet");
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

    serviceHook.startAndWait();
    try {
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

        // Execute the process method and block until it finished.
        Future<?> processFuture = processExecutor.submit(createProcessRunner(
          processQueue, processList, flowletContext.getProgram().getClassLoader()));
        while (!processFuture.isDone()) {
          try {
            // Wait uninterruptibly so that stop() won't kill the executing context
            // We need a timeout so that in case it takes too long to complete, we have chance to force quit it if
            // it is in shutdown sequence.
            Uninterruptibles.getUninterruptibly(processFuture, 30, TimeUnit.SECONDS);
          } catch (ExecutionException e) {
            LOG.error("Unexpected execution exception.", e);
          } catch (TimeoutException e) {
            // If in shutdown sequence, cancel the task by interrupting it.
            // Otherwise, just keep waiting until it completess
            if (!isRunning()) {
              LOG.info("Flowlet {} takes longer than 30 seconds to quite. Force quitting.",
                       flowletContext.getFlowletId());
              processFuture.cancel(true);
            }
          }
        }
      }

      // Clear the interrupted flag and execute Flowlet.destroy()
      Thread.interrupted();
    } catch (InterruptedException e) {
      // It is ok to do nothing: we are shutting down
    } finally {
      destroyFlowlet();
      serviceHook.stopAndWait();
    }
  }

  /**
   * Creates a {@link Runnable} for execution of calling flowlet process methods.
   */
  private Runnable createProcessRunner(final BlockingQueue<FlowletProcessEntry<?>> processQueue,
                                       final List<FlowletProcessEntry<?>> processList,
                                       final ClassLoader classLoader) {
    return new Runnable() {
      @Override
      public void run() {
        Thread.currentThread().setContextClassLoader(classLoader);
        for (FlowletProcessEntry<?> entry : processList) {
          if (!handleProcessEntry(entry, processQueue)) {
            processQueue.offer(entry);
          }
        }
      }
    };
  }

  /**
   * Invokes to perform dequeue and optionally invoke the user process input / tick method if dequeue gave a non
   * empty result.
   *
   * @param entry Contains information about the process method and queue.
   * @param processQueue The queue for queuing up all process input methods in a flowlet instance.
   * @param <T> Type of input of the process method accepted.
   *
   * @return {@code true} if the entry is handled completely (regardless of process result), {@code false} otherwise.
   */
  private <T> boolean handleProcessEntry(FlowletProcessEntry<T> entry,
                                      BlockingQueue<FlowletProcessEntry<?>> processQueue) {
    if (!entry.shouldProcess()) {
      return false;
    }

    ProcessMethod<T> processMethod = entry.getProcessSpec().getProcessMethod();
    if (processMethod.needsInput()) {
      flowletContext.getSystemMetrics().gauge("process.tuples.attempt.read", 1);
    }

    // Begin transaction and dequeue
    TransactionContext txContext = dataFabricFacade.createTransactionManager();
    try {
      txContext.start();

      try {
        InputDatum<T> input = entry.getProcessSpec().getQueueReader().dequeue(0, TimeUnit.MILLISECONDS);
        if (!input.needProcess()) {
          entry.backOff();
          // End the transaction if nothing in the queue
          txContext.finish();
          return false;
        }
        // Resetting back-off time to minimum back-off time,
        // since an entry to process was de-queued and most likely more entries will follow.
        entry.resetBackOff();

        if (!entry.isRetry()) {
          // Only increment the inflight count for non-retry entries.
          // The inflight would get decrement when the transaction committed successfully or input get ignored.
          // See the processMethodCallback function.
          inflight.getAndIncrement();
        }

        try {
          // Call the process method and commit the transaction. The current process entry will put
          // back to queue in the postProcess method (either a retry copy or itself).
          ProcessMethod.ProcessResult<?> result = processMethod.invoke(input);
          postProcess(processMethodCallback(processQueue, entry, input), txContext, input, result);
          return true;
        } catch (Throwable t) {
          // If exception thrown from invoke or postProcess, the inflight count would not be touched.
          // hence need to decrements here
          if (!entry.isRetry()) {
            inflight.decrementAndGet();
          }
        }

      } catch (Throwable t) {
        LOG.error("System failure: {}", flowletContext, t);
        try {
          txContext.abort();
        } catch (Throwable e) {
          LOG.error("Fail to abort transaction: {}", flowletContext, e);
        }
      }
    } catch (Throwable t) {
      LOG.error("Failed to start transaction.", t);
    }

    return false;
  }

  /**
   * Process the process result. This method never throws.
   */
  private void postProcess(ProcessMethodCallback callback, TransactionContext txContext,
                           InputDatum input, ProcessMethod.ProcessResult result) {
    InputContext inputContext = input.getInputContext();
    Throwable failureCause = null;
    FailureReason.Type failureType = FailureReason.Type.IO_ERROR;
    try {
      if (result.isSuccess()) {
        // If it is a retry input, force the dequeued entries into current transaction.
        if (input.getRetry() > 0) {
          input.reclaim();
        }
        txContext.finish();
      } else {
        failureCause = result.getCause();
        failureType = FailureReason.Type.USER;
        txContext.abort();
      }
    } catch (Throwable e) {
      LOG.error("Transaction operation failed: {}", e.getMessage(), e);
      failureType = FailureReason.Type.IO_ERROR;
      if (failureCause == null) {
        failureCause = e;
      }
      try {
        if (result.isSuccess()) {
          txContext.abort();
        }
      } catch (Throwable ex) {
        LOG.error("Fail to abort transaction: {}", inputContext, ex);
      }
    }

    try {
      if (failureCause == null) {
        callback.onSuccess(result.getEvent(), inputContext);
      } else {
        callback.onFailure(result.getEvent(), inputContext,
                           new FailureReason(failureType, failureCause.getMessage(), failureCause),
                           createInputAcknowledger(input));
      }
    } catch (Throwable t) {
      LOG.error("Failed to invoke callback.", t);
    }
  }

  private InputAcknowledger createInputAcknowledger(final InputDatum input) {
    return new InputAcknowledger() {
      @Override
      public void ack() throws TransactionFailureException {
        TransactionContext txContext = dataFabricFacade.createTransactionManager();
        txContext.start();
        input.reclaim();
        txContext.finish();
      }
    };
  }

  private void initFlowlet() throws InterruptedException {
    try {
      dataFabricFacade.createTransactionExecutor().execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          LOG.info("Initializing flowlet: " + flowletContext);
          flowlet.initialize(flowletContext);
          LOG.info("Flowlet initialized: " + flowletContext);
        }
      });
    } catch (TransactionFailureException e) {
      Throwable cause = e.getCause() == null ? e : e.getCause();
      LOG.error("Flowlet throws exception during flowlet initialize: " + flowletContext, cause);
      throw Throwables.propagate(cause);
    }
  }

  private void destroyFlowlet() {
    try {
      dataFabricFacade.createTransactionExecutor().execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          LOG.info("Destroying flowlet: " + flowletContext);
          flowlet.destroy();
          LOG.info("Flowlet destroyed: " + flowletContext);
        }
      });
    } catch (TransactionFailureException e) {
      Throwable cause = e.getCause() == null ? e : e.getCause();
      LOG.error("Flowlet throws exception during flowlet destroy: " + flowletContext, cause);
      // No need to propagate, as it is shutting down.
    } catch (InterruptedException e) {
      // No need to propagate, as it is shutting down.
    }
  }

  private <T> ProcessMethodCallback processMethodCallback(final BlockingQueue<FlowletProcessEntry<?>> processQueue,
                                                          final FlowletProcessEntry<T> processEntry,
                                                          final InputDatum<T> input) {
    // If it is generator flowlet, processCount is 1.
    final int processedCount = processEntry.getProcessSpec().getProcessMethod().needsInput() ? input.size() : 1;

    return new ProcessMethodCallback() {
      @Override
      public void onSuccess(Object object, InputContext inputContext) {
        try {
          gaugeEventProcessed(input.getQueueName());
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
                                       new ProcessSpecification<T>(new SingleItemQueueReader<T>(input),
                                                                   processEntry.getProcessSpec().getProcessMethod(),
                                                                   null));
          processQueue.offer(retryEntry);

        } else if (failurePolicy == FailurePolicy.IGNORE) {
          try {
            gaugeEventProcessed(input.getQueueName());
            inputAcknowledger.ack();
          } catch (Throwable t) {
            LOG.error("Fatal problem, fail to ack an input: {}", flowletContext, t);
          } finally {
            enqueueEntry();
            inflight.decrementAndGet();
          }
        }
      }

      private void enqueueEntry() {
        processQueue.offer(processEntry.resetRetry());
      }

      private void gaugeEventProcessed(QueueName inputQueueName) {
        if (processEntry.isTick()) {
          flowletContext.getSystemMetrics().gauge("process.ticks.processed", processedCount);
        } else if (inputQueueName == null) {
          flowletContext.getSystemMetrics().gauge("process.events.processed", processedCount);
        } else {
          String tag = "input." + inputQueueName.toString();
          flowletContext.getSystemMetrics().gauge("process.events.processed", processedCount, tag);
        }
      }
    };
  }
}
