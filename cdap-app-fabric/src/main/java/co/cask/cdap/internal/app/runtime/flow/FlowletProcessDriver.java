/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.flow;

import co.cask.cdap.api.flow.flowlet.Callback;
import co.cask.cdap.api.flow.flowlet.FailurePolicy;
import co.cask.cdap.api.flow.flowlet.FailureReason;
import co.cask.cdap.api.flow.flowlet.Flowlet;
import co.cask.cdap.api.flow.flowlet.InputContext;
import co.cask.cdap.app.queue.InputDatum;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.metrics.MetricTags;
import co.cask.cdap.common.metrics.MetricsCollector;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.internal.app.queue.SingleItemQueueReader;
import co.cask.cdap.internal.app.runtime.DataFabricFacade;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionFailureException;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * This class responsible invoking process methods of a {@link Flowlet}.
 */
final class FlowletProcessDriver extends AbstractExecutionThreadService {

  private static final Logger LOG = LoggerFactory.getLogger(FlowletProcessDriver.class);

  private final BasicFlowletContext flowletContext;
  private final DataFabricFacade dataFabricFacade;
  private final Callback txCallback;
  private final LoggingContext loggingContext;
  private final PriorityQueue<FlowletProcessEntry<?>> processQueue;

  private Thread runThread;
  private ExecutorService processExecutor;

  FlowletProcessDriver(BasicFlowletContext flowletContext,
                       DataFabricFacade dataFabricFacade,
                       Callback txCallback,
                       Collection<? extends ProcessSpecification<?>> processSpecifications) {
    this.flowletContext = flowletContext;
    this.dataFabricFacade = dataFabricFacade;
    this.txCallback = txCallback;
    this.loggingContext = flowletContext.getLoggingContext();

    processQueue = new PriorityQueue<FlowletProcessEntry<?>>(processSpecifications.size());
    for (ProcessSpecification<?> spec : processSpecifications) {
      processQueue.offer(FlowletProcessEntry.create(spec));
    }
  }

  /**
   * Copy constructor. Main purpose is to copy processQueue state. It's used for Flowlet suspend->resume.
   */
  FlowletProcessDriver(FlowletProcessDriver other) {
    // The state of other FlowletProcessDriver must be stopped.
    Preconditions.checkArgument(other.state() == State.TERMINATED, "FlowletProcessDriver is not terminated");

    this.flowletContext = other.flowletContext;
    this.dataFabricFacade = other.dataFabricFacade;
    this.txCallback = other.txCallback;
    this.loggingContext = other.loggingContext;
    this.processQueue = new PriorityQueue<FlowletProcessEntry<?>>(other.processQueue.size());
    Iterables.addAll(processQueue, other.processQueue);
  }

  @Override
  protected String getServiceName() {
    return getClass().getSimpleName() + "-" + flowletContext.getName() + "-" + flowletContext.getInstanceId();
  }

  @Override
  protected void startUp() throws Exception {
    runThread = Thread.currentThread();
    processExecutor = Executors.newSingleThreadExecutor(
      Threads.createDaemonThreadFactory(getServiceName() + "-executor"));
  }

  @Override
  protected void shutDown() throws Exception {
    processExecutor.shutdown();
  }

  @Override
  protected void triggerShutdown() {
    runThread.interrupt();
  }

  @Override
  protected void run() throws Exception {
    LoggingContextAccessor.setLoggingContext(loggingContext);

    // Collection for draining the processQueue for invoking process methods
    List<FlowletProcessEntry<?>> processList = Lists.newArrayListWithExpectedSize(processQueue.size() * 2);
    Runnable processRunner = createProcessRunner(processQueue, processList,
                                                 flowletContext.getProgram().getClassLoader());
    while (isRunning()) {
      try {
        // If the queue head need to wait, we had to wait.
        processQueue.peek().await();
      } catch (InterruptedException e) {
        // Triggered by shutdown, simply continue and let the isRunning() check to deal with that.
        continue;
      }

      processList.clear();
      // Drain the process queue so that all entries in the queue will be inspected to see if it's time to process
      drainQueue(processQueue, processList);

      // Execute the process method and block until it finished.
      Future<?> processFuture = processExecutor.submit(processRunner);
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
            LOG.info("Flowlet {} takes longer than 30 seconds to quit. Force quitting.", flowletContext.getFlowletId());
            processFuture.cancel(true);
          }
        }
      }
    }
  }

  private void drainQueue(PriorityQueue<FlowletProcessEntry<?>> queue,
                          List<? super FlowletProcessEntry<?>> collection) {
    FlowletProcessEntry<?> entry = queue.poll();
    while (entry != null) {
      collection.add(entry);
      entry = queue.poll();
    }
  }

  /**
   * Creates a {@link Runnable} for execution of calling flowlet process methods.
   */
  private Runnable createProcessRunner(final PriorityQueue<FlowletProcessEntry<?>> processQueue,
                                       final List<FlowletProcessEntry<?>> processList,
                                       final ClassLoader classLoader) {
    return new Runnable() {
      @Override
      public void run() {
        Thread.currentThread().setContextClassLoader(classLoader);
        for (FlowletProcessEntry<?> entry : processList) {
          if (!handleProcessEntry(entry, processQueue)) {
            // If an entry is not processed (because it's not the time yet), just put it back to the queue
            // Otherwise, it's up to the process result callback to handle re-enqueue of the entry. The callback
            // will determine what entry to put it back, as it can be the original entry or a retry entry wrapper,
            // depending on the process result.
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
                                         PriorityQueue<FlowletProcessEntry<?>> processQueue) {
    if (!entry.shouldProcess()) {
      return false;
    }

    ProcessMethod<T> processMethod = entry.getProcessSpec().getProcessMethod();
    if (processMethod.needsInput()) {
      flowletContext.getProgramMetrics().increment("process.tuples.attempt.read", 1);
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

        // Call the process method and commit the transaction. The current process entry will put
        // back to queue in the postProcess method (either a retry copy or itself).
        ProcessMethod.ProcessResult<?> result = processMethod.invoke(input);
        postProcess(processMethodCallback(processQueue, entry, input), txContext, input, result);
        return true;

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

  private <T> ProcessMethodCallback processMethodCallback(final PriorityQueue<FlowletProcessEntry<?>> processQueue,
                                                          final FlowletProcessEntry<T> processEntry,
                                                          final InputDatum<T> input) {
    // If it is generator flowlet, processCount is 1.
    final int processedCount = processEntry.getProcessSpec().getProcessMethod().needsInput() ? input.size() : 1;

    return new ProcessMethodCallback() {
      private final LoadingCache<String, MetricsCollector> queueMetricsCollectors = CacheBuilder.newBuilder()
        .expireAfterAccess(1, TimeUnit.HOURS)
        .build(new CacheLoader<String, MetricsCollector>() {
          @Override
          public MetricsCollector load(String key) throws Exception {
            return flowletContext.getProgramMetrics().childCollector(MetricTags.FLOWLET_QUEUE.getCodeName(), key);
          }
        });

      @Override
      public void onSuccess(Object object, InputContext inputContext) {
        try {
          gaugeEventProcessed(input.getQueueName());
          txCallback.onSuccess(object, inputContext);
        } catch (Throwable t) {
          LOG.error("Exception on onSuccess call: {}", flowletContext, t);
        } finally {
          enqueueEntry();
        }
      }

      @Override
      public void onFailure(Object inputObject, InputContext inputContext, FailureReason reason,
                            InputAcknowledger inputAcknowledger) {

        LOG.warn("Process failure: {}, {}, input: {}", flowletContext, reason.getMessage(), input, reason.getCause());
        FailurePolicy failurePolicy;
        try {
          flowletContext.getProgramMetrics().increment("process.errors", 1);
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
          }
        }
      }

      private void enqueueEntry() {
        processQueue.offer(processEntry.resetRetry());
      }

      private void gaugeEventProcessed(QueueName inputQueueName) {
        if (processEntry.isTick()) {
          flowletContext.getProgramMetrics().increment("process.ticks.processed", processedCount);
        } else if (inputQueueName == null) {
          flowletContext.getProgramMetrics().increment("process.events.processed", processedCount);
        } else {
          queueMetricsCollectors.getUnchecked("input." + inputQueueName.toString())
            .increment("process.events.processed", processedCount);
        }
      }
    };
  }
}
