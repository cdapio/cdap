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

package io.cdap.cdap.scheduler;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.ConflictException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.internal.app.runtime.schedule.ScheduleTaskRunner;
import io.cdap.cdap.internal.app.runtime.schedule.constraint.CheckableConstraint;
import io.cdap.cdap.internal.app.runtime.schedule.constraint.ConstraintContext;
import io.cdap.cdap.internal.app.runtime.schedule.constraint.ConstraintResult;
import io.cdap.cdap.internal.app.runtime.schedule.queue.Job;
import io.cdap.cdap.internal.app.runtime.schedule.queue.JobQueue;
import io.cdap.cdap.internal.app.runtime.schedule.queue.JobQueueTable;
import io.cdap.cdap.internal.app.runtime.schedule.store.Schedulers;
import io.cdap.cdap.internal.app.services.ProgramLifecycleService;
import io.cdap.cdap.internal.app.services.PropertiesResolver;
import io.cdap.cdap.internal.schedule.constraint.Constraint;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.spi.data.transaction.TransactionException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Polls the JobQueue, checks the jobs for constraint satisfaction, and launches them.
 */
class ConstraintCheckerService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(ConstraintCheckerService.class);

  private final Store store;
  private final ProgramLifecycleService lifecycleService;
  private final PropertiesResolver propertiesResolver;
  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final CConfiguration cConf;
  private final TransactionRunner transactionRunner;
  private ScheduleTaskRunner taskRunner;
  private ListeningExecutorService taskExecutorService;
  private volatile boolean stopping;
  private MetricsCollectionService metricsCollectionService;

  @Inject
  ConstraintCheckerService(Store store,
                           ProgramLifecycleService lifecycleService, PropertiesResolver propertiesResolver,
                           NamespaceQueryAdmin namespaceQueryAdmin,
                           CConfiguration cConf,
                           TransactionRunner transactionRunner,
                           MetricsCollectionService metricsCollectionService) {
    this.store = store;
    this.lifecycleService = lifecycleService;
    this.propertiesResolver = propertiesResolver;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.cConf = cConf;
    this.transactionRunner = transactionRunner;
    this.metricsCollectionService = metricsCollectionService;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting ConstraintCheckerService.");
    taskExecutorService = MoreExecutors.listeningDecorator(
      Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("constraint-checker-task-%d").build()));
    taskRunner = new ScheduleTaskRunner(store, lifecycleService, propertiesResolver, namespaceQueryAdmin, cConf);

    int numPartitions = cConf.getInt(Constants.Scheduler.JOB_QUEUE_NUM_PARTITIONS);
    for (int partition = 0; partition < numPartitions; partition++) {
      taskExecutorService.submit(new ConstraintCheckerThread(partition));
    }
    LOG.info("Started ConstraintCheckerService. state: " + state());
  }

  @Override
  protected void shutDown() throws Exception {
    stopping = true;
    LOG.info("Stopping ConstraintCheckerService.");
    try {
      // Shutdown the executor and wait for all pending task to be completed for max of 5 seconds
      taskExecutorService.shutdown();
      taskExecutorService.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    } finally {
      if (!taskExecutorService.isTerminated()) {
        taskExecutorService.shutdownNow();
      }
    }
    LOG.info("Stopped ConstraintCheckerService.");
  }

  private class ConstraintCheckerThread implements Runnable {
    private final RetryStrategy scheduleStrategy;
    private final int partition;
    private final Deque<Job> readyJobs = new ArrayDeque<>();
    private Job lastConsumed;
    private int failureCount;

    ConstraintCheckerThread(int partition) {
      // TODO: [CDAP-11370] Need to be configured in cdap-default.xml. Retry with delay ranging from 0.1s to 30s
      scheduleStrategy =
        io.cdap.cdap.common.service.RetryStrategies.exponentialDelay(100, 30000, TimeUnit.MILLISECONDS);
      this.partition = partition;
    }

    @Override
    public void run() {
      // TODO: how to retry the same jobs upon txConflict?

      while (!stopping) {
        try {
          long sleepTime = checkJobQueue();
          // Don't sleep if sleepTime returned is 0
          if (sleepTime > 0) {
            TimeUnit.MILLISECONDS.sleep(sleepTime);
          }
        } catch (InterruptedException e) {
          // sleep is interrupted, just exit without doing anything
        }
      }
    }

    /**
     * Check jobs in job queue for constraint satisfaction.
     *
     * @return sleep time in milliseconds before next fetch
     */
    private long checkJobQueue() {
      boolean emptyFetch = false;
      try {
        emptyFetch = TransactionRunners.run(transactionRunner, context -> {
          return checkJobConstraints(JobQueueTable.getJobQueue(context, cConf));
        });

        // run any ready jobs
        runReadyJobs();
        failureCount = 0;
      } catch (Exception e) {
        LOG.warn("Failed to check Job constraints. Will retry in next run", e);
        failureCount++;
      }

      // If there is any failure, delay the next fetch based on the strategy
      if (failureCount > 0) {
        // Exponential strategy doesn't use the time component, so doesn't matter what we passed in as startTime
        return scheduleStrategy.nextRetry(failureCount, 0);
      }

      // Sleep for 2 seconds if there's no jobs in the queue
      return emptyFetch && readyJobs.isEmpty() ? 2000L : 0L;
    }

    private boolean checkJobConstraints(JobQueue jobQueue) throws IOException {
      boolean emptyScan = true;

      try (CloseableIterator<Job> jobQueueIter = jobQueue.getJobs(partition, lastConsumed)) {
        Stopwatch stopWatch = new Stopwatch().start();
        // limit the batches of the scan to 1000ms
        while (!stopping && stopWatch.elapsedMillis() < 1000) {
          if (!jobQueueIter.hasNext()) {
            lastConsumed = null;
            return emptyScan;
          }
          Job job = jobQueueIter.next();
          lastConsumed = job;
          emptyScan = false;
          checkAndUpdateJob(jobQueue, job);
        }
      }
      return emptyScan;
    }

    private void checkAndUpdateJob(JobQueue jobQueue, Job job) throws IOException {
      long now = System.currentTimeMillis();
      if (job.isToBeDeleted()) {
        // only delete jobs that are pending trigger or pending constraint. If pending launch, the launcher will delete
        if ((job.getState() == Job.State.PENDING_CONSTRAINT ||
          // if pending trigger, we need to check if now - deletionTime > 2 * txTimeout. Otherwise the subscriber thread
          // might update this job concurrently (because its tx does not see the delete flag) and cause a conflict.
          // It's 2 * txTimeout for:
          // - the transaction the marked it as to be deleted
          // - the subscriber's transaction that may not have seen that change
          (job.getState() == Job.State.PENDING_TRIGGER &&
            now - job.getDeleteTimeMillis() > 2 * Schedulers.SUBSCRIBER_TX_TIMEOUT_MILLIS))) {
          jobQueue.deleteJob(job);
        }
        return;
      }
      if (now - job.getCreationTime() >= job.getSchedule().getTimeoutMillis() +
        2 * Schedulers.SUBSCRIBER_TX_TIMEOUT_MILLIS) {
        LOG.info("Deleted job {}, due to timeout value of {}.", job.getJobKey(), job.getSchedule().getTimeoutMillis());
        jobQueue.deleteJob(job);
        return;
      }
      if (job.getState() != Job.State.PENDING_CONSTRAINT) {
        return;
      }
      ConstraintResult.SatisfiedState satisfiedState = constraintsSatisfied(job, now);
      if (satisfiedState == ConstraintResult.SatisfiedState.NOT_SATISFIED) {
        return;
      }
      if (satisfiedState == ConstraintResult.SatisfiedState.NEVER_SATISFIED) {
        jobQueue.deleteJob(job);
        return;
      }
      jobQueue.transitState(job, Job.State.PENDING_LAUNCH);
      readyJobs.add(job);
    }

    private void runReadyJobs() {
      final Iterator<Job> readyJobsIter = readyJobs.iterator();
      while (readyJobsIter.hasNext() && !stopping) {
        final Job job = readyJobsIter.next();
        try {
          TransactionRunners.run(transactionRunner, context -> {
            runReadyJob(JobQueueTable.getJobQueue(context, cConf), job);
          }, TransactionException.class);
        } catch (TransactionException e) {
          LOG.warn("Failed to run program {} in schedule {}. Skip running this program.",
                   job.getSchedule().getProgramId(), job.getSchedule().getName(), e);
        }
        readyJobsIter.remove();
      }
    }

    // return whether or not the job should be removed from the readyJobs in-memory Deque
    private boolean runReadyJob(JobQueue jobQueue, Job job) throws IOException {
      // We should check the stored job's state (whether it actually is PENDING_LAUNCH), because
      // the schedule could have gotten deleted in the meantime or the transaction that marked it as PENDING_LAUNCH
      // may have failed / rolled back.
      Job storedJob = jobQueue.getJob(job.getJobKey());
      if (storedJob == null) {
        return true;
      }
      if (storedJob.isToBeDeleted() || storedJob.getState() != Job.State.PENDING_LAUNCH) {
        // If the storedJob.isToBeDeleted(), that means the schedule was deleted/updated before the state transition
        // from PENDING_CONSTRAINT to PENDING_LAUNCH is committed. We can just remove the job without launching it.
        // If job.isToBeDeleted() is false, that means this job is in PENDING_LAUNCH state before the schedule
        // is deleted/updated. We can still launch it.
        // The storedJob state could be something other than PENDING_LAUNCH, if the transaction aborted after added
        // the job to readyJobs (in-memory queue)
        jobQueue.deleteJob(job);
        return true;
      }

      try {
        taskRunner.launch(job);
        emitScheduleJobSuccessAndLatencyMetric(job.getSchedule().getScheduleId().getApplication(),
                                               job.getSchedule().getName(), job.getCreationTime());
      } catch (ConflictException e) {
        LOG.error("Skip job {} because it was rejected while launching: {}", job.getJobKey(), e.getMessage());
        emitScheduleJobFailureMetric(job.getSchedule().getScheduleId().getApplication(),
                                     job.getSchedule().getName());
      } catch (Exception e) {
        LOG.error("Skip launching job {} because the program {} encountered an exception while launching.",
                  job.getJobKey(), job.getSchedule().getProgramId(), e);
        emitScheduleJobFailureMetric(job.getSchedule().getScheduleId().getApplication(),
                                     job.getSchedule().getName());
      }
      // this should not have a conflict, because any updates to the job will first check to make sure that
      // it is not PENDING_LAUNCH
      jobQueue.deleteJob(job);
      return true;
    }

    private ConstraintResult.SatisfiedState constraintsSatisfied(Job job, long now) {
      ConstraintResult.SatisfiedState satisfiedState = ConstraintResult.SatisfiedState.SATISFIED;

      ConstraintContext constraintContext = new ConstraintContext(job, now, store);
      for (Constraint constraint : job.getSchedule().getConstraints()) {
        if (!(constraint instanceof CheckableConstraint)) {
          // this shouldn't happen, since implementation of Constraint in ProgramSchedule
          // should implement CheckableConstraint
          throw new IllegalArgumentException("Implementation of Constraint in ProgramSchedule" +
                                               " must implement CheckableConstraint");
        }

        CheckableConstraint abstractConstraint = (CheckableConstraint) constraint;
        ConstraintResult result = abstractConstraint.check(job.getSchedule(), constraintContext);
        if (result.getSatisfiedState() == ConstraintResult.NEVER_SATISFIED.getSatisfiedState()) {
          // if any of the constraints are NEVER_SATISFIED, return NEVER_SATISFIED
          return ConstraintResult.NEVER_SATISFIED.getSatisfiedState();
        }
        if (result.getSatisfiedState() == ConstraintResult.SatisfiedState.NOT_SATISFIED) {
          satisfiedState = ConstraintResult.SatisfiedState.NOT_SATISFIED;
        }
      }
      return satisfiedState;
    }

    private void emitScheduleJobSuccessAndLatencyMetric(String application, String schedule, long jobCreationTime) {
      MetricsContext collector = metricsCollectionService.getContext(
        getScheduleJobMetricsContext(application, schedule));
      collector.increment(Constants.Metrics.ScheduledJob.SCHEDULE_SUCCESS, 1);

      long currTime = System.currentTimeMillis();
      long latency = currTime - jobCreationTime;
      collector.gauge(Constants.Metrics.ScheduledJob.SCHEDULE_LATENCY, latency);
    }

    private void emitScheduleJobFailureMetric(String application, String schedule) {
      MetricsContext collector = metricsCollectionService.getContext(
        getScheduleJobMetricsContext(application, schedule));
      collector.increment(Constants.Metrics.ScheduledJob.SCHEDULE_FAILURE, 1);
    }

    private Map<String, String> getScheduleJobMetricsContext(String application, String schedule) {
      return ImmutableMap.of(
        Constants.Metrics.Tag.NAMESPACE, NamespaceId.SYSTEM.getEntityName(),
        Constants.Metrics.Tag.APP, application,
        Constants.Metrics.Tag.COMPONENT, "constraintchecker",
        Constants.Metrics.Tag.SCHEDULE, schedule);
    }
  }
}
