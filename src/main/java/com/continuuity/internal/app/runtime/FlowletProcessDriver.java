package com.continuuity.internal.app.runtime;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.flow.flowlet.FailurePolicy;
import com.continuuity.api.flow.flowlet.FailureReason;
import com.continuuity.api.flow.flowlet.Flowlet;
import com.continuuity.api.flow.flowlet.InputContext;
import com.continuuity.internal.app.queue.SingleItemQueueReader;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * This class responsible invoking process methods one by one and commit the post process transaction.
 */
public class FlowletProcessDriver extends AbstractExecutionThreadService {

  private static final Logger LOGGER = LoggerFactory.getLogger(FlowletProcessDriver.class);
  private static final int TX_EXECUTOR_POOL_SIZE = 2;

  private static final long BACKOFF_MIN = TimeUnit.MILLISECONDS.toNanos(1); // 1ms
  private static final long BACKOFF_MAX = TimeUnit.SECONDS.toNanos(2);      // 2 seconds
  private static final int BACKOFF_EXP = 2;
  private static int PROCESS_MAX_RETRY = 10;

  private final Flowlet flowlet;
  private final BasicFlowletContext flowletContext;
  private final Collection<ProcessSpecification> processSpecs;
  private final TransactionCallback txCallback;
  private ExecutorService transactionExecutor;

  public FlowletProcessDriver(Flowlet flowlet,
                              BasicFlowletContext flowletContext,
                              Collection<ProcessSpecification> processSpecs,
                              TransactionCallback txCallback) {
    this.flowlet = flowlet;
    this.flowletContext = flowletContext;
    this.processSpecs = processSpecs;
    this.txCallback = txCallback;
  }

  @Override
  protected void startUp() throws Exception {
    if (flowletContext.isAsyncMode()) {
      transactionExecutor = Executors.newFixedThreadPool(TX_EXECUTOR_POOL_SIZE,
                                                         new ThreadFactoryBuilder()
                                                           .setDaemon(true)
                                                           .setNameFormat("tx-executor-%d")
                                                           .build());
    } else {
      transactionExecutor = MoreExecutors.sameThreadExecutor();
    }
  }

  @Override
  protected void shutDown() throws Exception {
    // TODO
  }

  @Override
  protected void triggerShutdown() {
    // TODO
  }

  @Override
  protected void run() throws Exception {
    initFlowlet();

    // Insert all into priority queue, ordered by next deque time.
    PriorityQueue<ProcessEntry> processQueue = new PriorityQueue<ProcessEntry>(processSpecs.size());
    for (ProcessSpecification spec : processSpecs) {
      processQueue.offer(new ProcessEntry(spec));
    }

    while (isRunning()) {
      // TODO: Suspend Flowlet
      long startTime = System.nanoTime();
      ProcessEntry entry = processQueue.poll();

      long waitTime = entry.nextDeque - startTime;
      if (waitTime > 0) {
        TimeUnit.NANOSECONDS.sleep(waitTime);
      }
      entry.nextDeque = 0;
      InputDatum input = entry.processSpec.getQueueReader().dequeue();

      try {
        if (input.isEmpty()) {
          entry.backOff();
          continue;
        }

        try {
          // Call the process method and commit the transaction
          entry.processSpec.getProcessMethod().invoke(input)
            .commit(transactionExecutor, processMethodCallback(processQueue, entry, input));

        } catch (Throwable t) {
          LOGGER.error(String.format("Fail to invoke process method: %s", entry.processSpec));
        }
      } finally {
        // If it is not a retry entry, always put it back to the queue, otherwise let the committer do the job.
        if (!entry.isRetry()) {
          processQueue.offer(entry);
        }
      }
    }

    destroyFlowlet();
  }

  private void initFlowlet() {
    try {
      flowlet.initialize(flowletContext);
    } catch (Throwable t) {
      LOGGER.error("Flowlet throws exception during flowlet initialize.", t);
      throw Throwables.propagate(t);
    }
  }

  private void destroyFlowlet() {
    try {
      flowlet.destroy();
    } catch (Throwable t) {
      LOGGER.error("Flowlet throws exception during flowlet destroy.", t);
      throw Throwables.propagate(t);
    }
  }

  private PostProcess.Callback processMethodCallback(final PriorityQueue<ProcessEntry> processQueue,
                                                     final ProcessEntry processEntry,
                                                     final InputDatum input) {
    return new PostProcess.Callback() {
      @Override
      public void onSuccess(Object object, InputContext inputContext) {
        try {
          txCallback.onSuccess(object, inputContext);
        } catch (Throwable t) {
          LOGGER.info("Exception on onSuccess call.", t);
        }
      }

      @Override
      public void onFailure(Object inputObject, InputContext inputContext, FailureReason reason,
                            PostProcess.InputAcknowledger inputAcknowledger) {

        FailurePolicy failurePolicy;
        try {
          failurePolicy = txCallback.onFailure(inputObject, inputContext, reason);
        } catch (Throwable t) {
          LOGGER.info("Exception on onFailure call.", t);
          failurePolicy = FailurePolicy.RETRY;
        }

        if (input.getRetry() >= PROCESS_MAX_RETRY) {
          failurePolicy = FailurePolicy.IGNORE;
        }

        if (failurePolicy == FailurePolicy.RETRY) {
          ProcessEntry retryEntry = processEntry.isRetry() ?
            processEntry :
            new ProcessEntry(
              new ProcessSpecification(new SingleItemQueueReader(input),
                                       processEntry.processSpec.getProcessMethod()),
              true);

          processQueue.offer(retryEntry);

        } else if (failurePolicy == FailurePolicy.IGNORE) {
          try {
            inputAcknowledger.ack();
          } catch (OperationException e) {
            LOGGER.error("Fatal problem, fail to ack an input.", e);
          }
        }
      }
    };
  }

  private static final class ProcessEntry implements Comparable<ProcessEntry> {
    private final ProcessSpecification processSpec;
    private final boolean retry;
    private long nextDeque;
    private long currentBackOff = BACKOFF_MIN;

    private ProcessEntry(ProcessSpecification processSpec) {
      this(processSpec, false);
    }

    private ProcessEntry(ProcessSpecification processSpec, boolean retry) {
      this.processSpec = processSpec;
      this.retry = retry;
    }

    public boolean isRetry() {
      return retry;
    }

    @Override
    public int compareTo(ProcessEntry o) {
      if (nextDeque == o.nextDeque) {
        return 0;
      }
      return nextDeque > o.nextDeque ? 1 : -1;
    }

    public void backOff() {
      nextDeque = System.nanoTime() + currentBackOff;
      currentBackOff = Math.min(currentBackOff * BACKOFF_EXP, BACKOFF_MAX);
    }
  }
}
