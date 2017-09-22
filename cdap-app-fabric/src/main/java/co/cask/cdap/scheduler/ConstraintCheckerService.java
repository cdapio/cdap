/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.scheduler;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.ApplicationNotFoundException;
import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.ProgramNotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.common.service.RetryStrategy;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.data2.transaction.TxCallable;
import co.cask.cdap.internal.app.runtime.schedule.ScheduleTaskRunner;
import co.cask.cdap.internal.app.runtime.schedule.TaskExecutionException;
import co.cask.cdap.internal.app.runtime.schedule.constraint.CheckableConstraint;
import co.cask.cdap.internal.app.runtime.schedule.constraint.ConstraintContext;
import co.cask.cdap.internal.app.runtime.schedule.constraint.ConstraintResult;
import co.cask.cdap.internal.app.runtime.schedule.queue.Job;
import co.cask.cdap.internal.app.runtime.schedule.queue.JobQueueDataset;
import co.cask.cdap.internal.app.runtime.schedule.store.Schedulers;
import co.cask.cdap.internal.app.services.ProgramLifecycleService;
import co.cask.cdap.internal.app.services.PropertiesResolver;
import co.cask.cdap.internal.schedule.constraint.Constraint;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Polls the JobQueue, checks the jobs for constraint satisfaction, and launches them.
 */
class ConstraintCheckerService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(ConstraintCheckerService.class);

  private final Transactional transactional;
  private final DatasetFramework datasetFramework;
  private final MultiThreadDatasetCache multiThreadDatasetCache;
  private final Store store;
  private final ProgramLifecycleService lifecycleService;
  private final PropertiesResolver propertiesResolver;
  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final CConfiguration cConf;
  private ScheduleTaskRunner taskRunner;
  private ListeningExecutorService taskExecutorService;
  private volatile boolean stopping = false;

  @Inject
  ConstraintCheckerService(Store store,
                           ProgramLifecycleService lifecycleService, PropertiesResolver propertiesResolver,
                           NamespaceQueryAdmin namespaceQueryAdmin,
                           CConfiguration cConf,
                           DatasetFramework datasetFramework,
                           TransactionSystemClient txClient) {
    this.store = store;
    this.lifecycleService = lifecycleService;
    this.propertiesResolver = propertiesResolver;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.cConf = cConf;
    this.multiThreadDatasetCache = new MultiThreadDatasetCache(
      new SystemDatasetInstantiator(datasetFramework), txClient,
      NamespaceId.SYSTEM, ImmutableMap.<String, String>of(), null, null);
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(multiThreadDatasetCache),
      RetryStrategies.retryOnConflict(20, 100)
    );
    this.datasetFramework = datasetFramework;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting ConstraintCheckerService.");
    taskExecutorService = MoreExecutors.listeningDecorator(
      Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("constraint-checker-task").build()));
    taskRunner = new ScheduleTaskRunner(store, lifecycleService, propertiesResolver,
                                        taskExecutorService, namespaceQueryAdmin, cConf);

    int numPartitions = Schedulers.getJobQueue(multiThreadDatasetCache, datasetFramework).getNumPartitions();
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

  private static class FailedJob {
    private final Job job;
    private final long startTime;
    private int failureCount;
    private long nextRetryTime;

    FailedJob(Job job, long startTime, long nextRetryTime) {
      this.job = job;
      this.startTime = startTime;
      this.nextRetryTime = nextRetryTime;
      failureCount = 1;
    }

    public Job getJob() {
      return job;
    }

    public long getStartTime() {
      return startTime;
    }

    public long getNextRetryTime() {
      return nextRetryTime;
    }

    public int getFailureCount() {
      return failureCount;
    }

    public void incrementFailureCount() {
      failureCount++;
    }

    public void setNextRetryTime(long nextRetryTime) {
      this.nextRetryTime = nextRetryTime;
    }
  }

  private class ConstraintCheckerThread implements Runnable {
    private final RetryStrategy scheduleStrategy;
    private final RetryStrategy retryFailedJobStartegy;
    private final int partition;
    private final Deque<Job> readyJobs = new ArrayDeque<>();
    private final Deque<FailedJob> failedJobs = new ArrayDeque<>();
    private JobQueueDataset jobQueue;
    private Job lastConsumed;
    private int failureCount;

    ConstraintCheckerThread(int partition) {
      // TODO: [CDAP-11370] Need to be configured in cdap-default.xml. Retry with delay ranging from 0.1s to 30s
      scheduleStrategy =
        co.cask.cdap.common.service.RetryStrategies.exponentialDelay(100, 30000, TimeUnit.MILLISECONDS);
      // Limit only 10 retries of failed jobs with delay ranging from 10s to 120s
      retryFailedJobStartegy =
        co.cask.cdap.common.service.RetryStrategies.limit(
          10, co.cask.cdap.common.service.RetryStrategies.exponentialDelay(10, 120, TimeUnit.SECONDS));
      this.partition = partition;
    }

    @Override
    public void run() {
      // TODO: how to retry the same jobs upon txConflict?
      jobQueue = Schedulers.getJobQueue(multiThreadDatasetCache, datasetFramework);

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
        emptyFetch = Transactions.execute(transactional, new TxCallable<Boolean>() {
          @Override
          public Boolean call(DatasetContext context) throws Exception {
            return checkJobConstraints();
          }
        });

        // run any ready jobs
        runReadyJobs();
        rerunFailedJobs();
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

    private boolean checkJobConstraints() throws Exception {
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

    private void checkAndUpdateJob(JobQueueDataset jobQueue, Job job) {
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
          Transactions.execute(transactional, new TxCallable<Void>() {
            @Override
            public Void call(DatasetContext context) throws Exception {
              if (runReadyJob(job)) {
                readyJobsIter.remove();
              }
              return null;
            }
          });
        } catch (TransactionFailureException e) {
          readyJobsIter.remove();
          long current = System.currentTimeMillis();
          long nextRetryWaitMillis = retryFailedJobStartegy.nextRetry(1, current);
          if (nextRetryWaitMillis < 0) {
            LOG.warn("Failed to run program {} in schedule {}. Skip running this program.",
                     job.getSchedule().getProgramId(), job.getSchedule().getName(), e);
            continue;
          }
          LOG.warn("Failed to run program {} in schedule {}. Will retry in {}ms.",
                   job.getSchedule().getProgramId(), job.getSchedule().getName(), nextRetryWaitMillis, e);
          // add the failed job to the queue to wait for retry
          failedJobs.add(new FailedJob(job, current, current + nextRetryWaitMillis));
        }
      }
    }

    private void rerunFailedJobs() {
      final Iterator<FailedJob> failedJobsIter = failedJobs.iterator();
      while (failedJobsIter.hasNext() && !stopping) {
        final FailedJob failedJob = failedJobsIter.next();
        long current = System.currentTimeMillis();
        if (failedJob.getNextRetryTime() > current) {
          continue;
        }
        try {
          Transactions.execute(transactional, new TxCallable<Void>() {
            @Override
            public Void call(DatasetContext context) throws Exception {
              if (runReadyJob(failedJob.getJob())) {
                failedJobsIter.remove();
              }
              return null;
            }
          });
        } catch (TransactionFailureException e) {
          failedJob.incrementFailureCount();
          long nextRetryWaitMillis = retryFailedJobStartegy.nextRetry(failedJob.getFailureCount(),
                                                                      failedJob.getStartTime());
          if (nextRetryWaitMillis < 0) {
            failedJobsIter.remove();
            LOG.warn("Failed to run program {} in schedule {}. Skip running this program.",
                     failedJob.getJob().getSchedule().getProgramId(), failedJob.getJob().getSchedule().getName(), e);
            continue;
          }
          failedJob.setNextRetryTime(current + nextRetryWaitMillis);
          LOG.warn("Failed to run program {} in schedule {}. Will retry in {} millis.",
                   failedJob.getJob().getSchedule().getProgramId(), failedJob.getJob().getSchedule().getName(),
                   nextRetryWaitMillis, e);
        }
      }
    }

    // return whether or not the job should be removed from the readyJobs in-memory Deque
    private boolean runReadyJob(Job job) throws Exception {
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
      } catch (NamespaceNotFoundException | TaskExecutionException e) {
        if (e instanceof TaskExecutionException && !(e.getCause() instanceof ProgramNotFoundException
          || e.getCause() instanceof ApplicationNotFoundException)) {
          // Only catch the TaskExecutionException if its cause is ProgramNotFoundException
          // or ApplicationNotFoundException
          throw e;
        }
        // This can happen when the namespace or app containing the program is deleted, but the job is already in
        // PENDING_LAUNCH state, so the job is not marked as to be deleted. Simply catch the TaskExecutionException
        // to skip retrying on failure and continue to delete this job
        LOG.info("Skip launching job {} because the program {} it tries to launch no longer exists",
                 job.getJobKey(), job.getSchedule().getProgramId());
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

  }
}
