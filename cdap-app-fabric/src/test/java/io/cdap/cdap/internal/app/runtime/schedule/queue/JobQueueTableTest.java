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

package io.cdap.cdap.internal.app.runtime.schedule.queue;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.schedule.Trigger;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramSchedule;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramScheduleMeta;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramScheduleRecord;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramScheduleStatus;
import io.cdap.cdap.internal.app.runtime.schedule.store.Schedulers;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.OrTriggerBuilder;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.PartitionTrigger;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.TimeTrigger;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.WorkflowId;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import org.junit.After;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests for {@link JobQueueTable}.
 */
public abstract class JobQueueTableTest {
  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();
  private static final Logger LOG = LoggerFactory.getLogger(JobQueueTableTest.class);

  private static final NamespaceId TEST_NS = new NamespaceId("jobQueueTest");
  private static final ApplicationId APP_ID = TEST_NS.app("app1");
  private static final WorkflowId WORKFLOW_ID = APP_ID.workflow("wf1");
  private static final DatasetId DATASET_ID = TEST_NS.dataset("pfs1");
  private static final DatasetId DATASET2_ID = TEST_NS.dataset("pfs2");

  private static final ProgramSchedule SCHED1 = new ProgramSchedule("SCHED1", "one partition schedule", WORKFLOW_ID,
                                                                    ImmutableMap.of("prop3", "abc"),
                                                                    new PartitionTrigger(DATASET_ID, 1),
                                                                    ImmutableList.of());
  private static final ProgramSchedule SCHED2 = new ProgramSchedule("SCHED2", "time schedule", WORKFLOW_ID,
                                                                    ImmutableMap.of("prop3", "abc"),
                                                                    new TimeTrigger("* * * * *"),
                                                                    ImmutableList.of());
  private static final Trigger TRIGGER =
    new OrTriggerBuilder(new PartitionTrigger(DATASET_ID, 6),
                         new PartitionTrigger(DATASET2_ID, 1))
      .build(APP_ID.getNamespace(), APP_ID.getApplication(), APP_ID.getVersion());
  private static final ProgramSchedule SCHED3 = new ProgramSchedule("SCHED3", "three partitions schedule", WORKFLOW_ID,
                                                                    ImmutableMap.of("prop1", "abc1"),
                                                                    TRIGGER,
                                                                    ImmutableList.of());

  private static final Job SCHED1_JOB = new SimpleJob(SCHED1, 0, System.currentTimeMillis(),
                                                      Lists.newArrayList(),
                                                      Job.State.PENDING_TRIGGER, 0L);
  private static final Job SCHED2_JOB = new SimpleJob(SCHED2, 0, System.currentTimeMillis(),
                                                      Lists.newArrayList(),
                                                      Job.State.PENDING_TRIGGER, 0L);

  protected abstract TransactionRunner getTransactionRunner();
  protected abstract CConfiguration getCConf();

  private final TransactionRunner transactionRunner = getTransactionRunner();

  @After
  public void tearDown() {
    TransactionRunners.run(transactionRunner, context -> {
      JobQueueTable jobQueue = JobQueueTable.getJobQueue(context, getCConf());
      for (Job job : getAllJobs(jobQueue, true)) {
        jobQueue.deleteJob(job);
      }
    });
  }

  @Test
  public void testMessageId() {
    final String topic1 = "topic1";
    final String topic2 = "topic2";
    final String messageIdToPut = "messageIdToPut";

    TransactionRunners.run(transactionRunner, context -> {
      JobQueueTable jobQueue = JobQueueTable.getJobQueue(context, getCConf());
      // without first setting the message Id, a get will return null
      Assert.assertNull(jobQueue.retrieveSubscriberState(topic1));

      // test set and get
      jobQueue.persistSubscriberState(topic1, messageIdToPut);
      Assert.assertEquals(messageIdToPut, jobQueue.retrieveSubscriberState(topic1));

      // the message id for a different topic should still be null
      Assert.assertNull(jobQueue.retrieveSubscriberState(topic2));
    });

    TransactionRunners.run(transactionRunner, context -> {
      JobQueueTable jobQueue = JobQueueTable.getJobQueue(context, getCConf());
      // the message Id should be retrievable across transactions
      Assert.assertEquals(messageIdToPut, jobQueue.retrieveSubscriberState(topic1));
      Assert.assertNull(jobQueue.retrieveSubscriberState(topic2));
    });
  }

  @Test
  public void testJobQueue() {
    TransactionRunners.run(transactionRunner, context -> {
      JobQueueTable jobQueue = JobQueueTable.getJobQueue(context, getCConf());
      // should be 0 jobs in the JobQueue to begin with
      Assert.assertEquals(0, getAllJobs(jobQueue).size());
      Assert.assertEquals(0, toSet(jobQueue.getJobsForSchedule(SCHED1.getScheduleId())).size());
      Assert.assertEquals(0, toSet(jobQueue.getJobsForSchedule(SCHED2.getScheduleId())).size());

      // put a job for SCHED1, and check that it is in 'getJobs' and 'getJobsForSchedule'
      jobQueue.put(SCHED1_JOB);

      // test get
      Assert.assertEquals(SCHED1_JOB, jobQueue.getJob(SCHED1_JOB.getJobKey()));

      Assert.assertEquals(ImmutableSet.of(SCHED1_JOB), getAllJobs(jobQueue));
      Assert.assertEquals(ImmutableSet.of(SCHED1_JOB), toSet(jobQueue.getJobsForSchedule(SCHED1.getScheduleId())));

      // the job for SCHED1 should not be in 'getJobsForSchedule' for SCHED2
      Assert.assertEquals(0, toSet(jobQueue.getJobsForSchedule(SCHED2.getScheduleId())).size());

      // put a job for SCHED2 and verify that it is also in the returned list
      jobQueue.put(SCHED2_JOB);

      Assert.assertEquals(ImmutableSet.of(SCHED1_JOB, SCHED2_JOB), getAllJobs(jobQueue));
      Assert.assertEquals(ImmutableSet.of(SCHED2_JOB), toSet(jobQueue.getJobsForSchedule(SCHED2.getScheduleId())));

      // mark job for SCHED1 for deletion and assert that it is no longer in 'getJobs'
      jobQueue.markJobsForDeletion(SCHED1.getScheduleId(), System.currentTimeMillis());
    });

    // doing the last assertion in a separate transaction to workaround CDAP-3511
    TransactionRunners.run(transactionRunner, context -> {
      JobQueueTable jobQueue = JobQueueTable.getJobQueue(context, getCConf());
      Assert.assertEquals(ImmutableSet.of(SCHED2_JOB), getAllJobs(jobQueue));
      Assert.assertEquals(0, toSet(jobQueue.getJobsForSchedule(SCHED1.getScheduleId())).size());
      Assert.assertEquals(ImmutableSet.of(SCHED2_JOB), toSet(jobQueue.getJobsForSchedule(SCHED2.getScheduleId())));
    });

    // now actually delete all jobs that are marked for deletion
    TransactionRunners.run(transactionRunner, context -> {
      JobQueueTable jobQueue = JobQueueTable.getJobQueue(context, getCConf());
      for (Job job : getAllJobs(jobQueue, true)) {
        if (job.isToBeDeleted()) {
          jobQueue.deleteJob(job);
        }
      }
    });

    // and validate that the job is completely gone
    TransactionRunners.run(transactionRunner, context -> {
      JobQueueTable jobQueue = JobQueueTable.getJobQueue(context, getCConf());
      Assert.assertEquals(ImmutableSet.of(SCHED2_JOB), getAllJobs(jobQueue, true));
      Assert.assertEquals(0, toSet(jobQueue.getJobsForSchedule(SCHED1.getScheduleId())).size());
      Assert.assertEquals(ImmutableSet.of(SCHED2_JOB), toSet(jobQueue.getJobsForSchedule(SCHED2.getScheduleId())));
    });
  }

  @Test
  public void testGetAllJobs() {
    // Test that getJobs can be called even when there are messageIds persisted in the same dataset.
    // This tests rowkey isolation, since getJobs is a scan across the entire job row-space.
    TransactionRunners.run(transactionRunner, context -> {
      JobQueueTable jobQueue = JobQueueTable.getJobQueue(context, getCConf());
      // should be 0 jobs in the JobQueue to begin with
      Assert.assertEquals(0, getAllJobs(jobQueue).size());

      jobQueue.persistSubscriberState("someTopic", "someMessageId");

      // put a job for SCHED1, and check that it is in 'getJobs' and 'getJobsForSchedule'
      jobQueue.put(SCHED1_JOB);

      Assert.assertEquals(ImmutableSet.of(SCHED1_JOB), getAllJobs(jobQueue));
      Assert.assertEquals(ImmutableSet.of(SCHED1_JOB), toSet(jobQueue.fullScan()));
    });
  }


  @Test
  public void testJobQueueIteration() {
    // Test that getJobs can be leveraged to iterate over the JobQueue partially (and across transactions).
    // The Job last consumed from the Iterator returned in the first iteration can be passed to the next call to resume
    // scanning from
    TransactionRunners.run(transactionRunner, context -> {
      JobQueueTable jobQueue = JobQueueTable.getJobQueue(context, getCConf());
      // should be 0 jobs in the JobQueue to begin with
      Assert.assertEquals(0, getAllJobs(jobQueue).size());

      Multimap<Integer, Job> jobsByPartition = HashMultimap.create();
      long now = 1494353984967L;
      for (int i = 0; i < 100; i++) {
        ProgramSchedule schedule = new ProgramSchedule("sched" + i, "one partition schedule", WORKFLOW_ID,
                                                       ImmutableMap.of(),
                                                       new PartitionTrigger(DATASET_ID, 1),
                                                       ImmutableList.of());
        Job job = new SimpleJob(schedule, i, now + i, ImmutableList.of(),
                                Job.State.PENDING_TRIGGER, 0L);
        jobsByPartition.put(jobQueue.getPartition(schedule.getScheduleId()), job);
        jobQueue.put(job);
      }

      // in partition 0, there should be at least two Jobs for us to even test resumption of job queue iteration
      Set<Job> partitionZeroJobs = new HashSet<>(jobsByPartition.get(0));
      Assert.assertTrue(partitionZeroJobs.size() > 1);

      // sanity check that all 100 jobs are in the JobQueue
      Assert.assertEquals(jobsByPartition.size(), getAllJobs(jobQueue).size());

      Assert.assertEquals(partitionZeroJobs, toSet(jobQueue.getJobs(0, null)));

      // consume just 1 job in the first partition. Then, use it to specify the exclusive starting point in the
      // next call to getJobs
      Job firstConsumedJob;
      try (CloseableIterator<Job> partitionZeroJobsIter = jobQueue.getJobs(0, null)) {
        Assert.assertTrue(partitionZeroJobsIter.hasNext());
        firstConsumedJob = partitionZeroJobsIter.next();
      }

      // the Jobs consumed in the second iteration should be all except the Job consumed in the first iteration
      Set<Job> consumedInSecondIteration = toSet(jobQueue.getJobs(0, firstConsumedJob));
      Assert.assertEquals(partitionZeroJobs.size() - 1, consumedInSecondIteration.size());

      Set<Job> consumedJobs = new HashSet<>(consumedInSecondIteration);
      consumedJobs.add(firstConsumedJob);
      Assert.assertEquals(partitionZeroJobs, consumedJobs);
    });
  }

  @Test
  public void testAddNotifications() {
    TransactionRunners.run(transactionRunner, context -> {
      JobQueueTable jobQueue = JobQueueTable.getJobQueue(context, getCConf());
      // should be 0 jobs in the JobQueue to begin with
      Assert.assertEquals(0, getAllJobs(jobQueue, false).size());

      // Construct a partition notification with DATASET_ID
      Notification notification = Notification.forPartitions(DATASET_ID, ImmutableList.of());

      Assert.assertNull(jobQueue.getJob(SCHED1_JOB.getJobKey()));

      jobQueue.put(SCHED1_JOB);
      Assert.assertEquals(SCHED1_JOB, jobQueue.getJob(SCHED1_JOB.getJobKey()));

      // Since notification and SCHED1 have the same dataset id DATASET_ID, notification will be added to
      // SCHED1_JOB, which is a job in SCHED1
      jobQueue.addNotification(
        new ProgramScheduleRecord(SCHED1, new ProgramScheduleMeta(ProgramScheduleStatus.SCHEDULED, 0L)),
        notification);
      Assert.assertEquals(ImmutableList.of(notification), jobQueue.getJob(SCHED1_JOB.getJobKey()).getNotifications());
    });
  }

  @Test
  public void testAddConcurrentNotifications() throws Exception {
    TransactionRunners.run(transactionRunner, context -> {
      JobQueueTable jobQueue = JobQueueTable.getJobQueue(context, getCConf());
      // should be 0 jobs in the JobQueue to begin with
      Assert.assertEquals(0, getAllJobs(jobQueue, false).size());
    });

     // Add concurrent notifications to SCHED3
    int numThreads = 5;
    addConcurrentNotifications(numThreads, SCHED3, (Callable<Void>) () -> null);

    // There should be only one job at the end with all the notifications attached to it
    Set<Job> actualJobs = TransactionRunners.run(transactionRunner, context -> {
      JobQueueTable jobQueue = JobQueueTable.getJobQueue(context, getCConf());
      return toSet(jobQueue.getJobsForSchedule(SCHED3.getScheduleId()));
    });
    Assert.assertEquals(1, actualJobs.size());

    List<Notification> expectedNotifications = new ArrayList<>();
    for (int i = 0; i < numThreads; i++) {
      expectedNotifications.add(Notification.forPartitions(DATASET_ID, ImmutableList.of()));
    }
    Job expectedJob = new SimpleJob(SCHED3, 0, actualJobs.iterator().next().getCreationTime(),
                                    expectedNotifications,
                                    Job.State.PENDING_TRIGGER, 0L);
    Assert.assertEquals(Collections.singleton(expectedJob), actualJobs);
  }

  private void addNotificationToSchedule(ProgramSchedule programSchedule) {
    TransactionRunners.run(transactionRunner, context -> {
      JobQueueTable jobQueue = JobQueueTable.getJobQueue(context, getCConf());
      // Construct a partition notification with DATASET_ID
      Notification notification = Notification.forPartitions(DATASET_ID, ImmutableList.of());
      jobQueue.addNotification(
        new ProgramScheduleRecord(programSchedule, new ProgramScheduleMeta(ProgramScheduleStatus.SCHEDULED, 0L)),
        notification);
    });
  }

  private <T> T addConcurrentNotifications(int numThreads, ProgramSchedule schedule,
                                           Callable<T> concurrentCallable) throws Exception {
    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(numThreads);
    try {
      for (int i = 0; i < numThreads; ++i) {
        executorService.submit(() -> {
          try {
            startLatch.await();
            addNotificationToSchedule(schedule);
            doneLatch.countDown();
          } catch (Exception e) {
            LOG.error("Error adding notification to schedule {}", SCHED3, e);
          }
        });
      }

      startLatch.countDown();
      T result = concurrentCallable.call();
      Assert.assertTrue(doneLatch.await(30, TimeUnit.SECONDS));
      return result;
    } finally {
      executorService.shutdown();
    }
  }

  @Test
  public void testConcurrentDeleteUpdate() throws Exception {
    Job expectedJob = new SimpleJob(SCHED3, 0, System.currentTimeMillis(),
                                    Collections.emptyList(),
                                    Job.State.PENDING_TRIGGER, 0L);
    TransactionRunners.run(transactionRunner, context -> {
      JobQueueTable jobQueue = JobQueueTable.getJobQueue(context, getCConf());
      // should be 0 jobs in the JobQueue to begin with
      Assert.assertEquals(0, getAllJobs(jobQueue, false).size());
      // Add a job for SCHED3
      jobQueue.put(expectedJob);
    });

    // Add concurrent notifications to SCHED3
    int numThreads = 15;
    Integer numTries = addConcurrentNotifications(numThreads, SCHED3, () -> {
      // While the concurrent updates are happening, mark SCHED3 for deletion
      AtomicInteger tries = new AtomicInteger(0);
      TransactionRunners.run(transactionRunner, context -> {
        JobQueueTable jobQueue = JobQueueTable.getJobQueue(context, getCConf());
        tries.incrementAndGet();
        jobQueue.markJobsForDeletion(SCHED3.getScheduleId(), System.currentTimeMillis());
      });
      return tries.get();
    });

    // Assert that there was no conflict when running markJobsForDeletion.
    // There should be only one try if there was no conflict
    Assert.assertEquals(1, numTries.intValue());

    // There should be only one job for SCHED3 marked for deletion,
    // and at most one active job for SCHED3 (0 active jobs if markJobsForDeletion happens after all notifications
    // are applied, or 1 job otherwise)
    Set<Job> actualJobs = TransactionRunners.run(transactionRunner, context -> {
      JobQueueTable jobQueue = JobQueueTable.getJobQueue(context, getCConf());
      return getAllJobs(jobQueue, true);
    });
    Assert.assertFalse(actualJobs.isEmpty());
    int deletedJobs = 0;
    int activeJobs = 0;
    for (Job actualJob : actualJobs) {
      if (actualJob.isToBeDeleted()) {
        deletedJobs++;
      } else {
        activeJobs++;
      }
    }
    Assert.assertEquals(1, deletedJobs);
    Assert.assertTrue(activeJobs <= 1);
  }

  @Test
  public void testJobTimeout() {
    TransactionRunners.run(transactionRunner, context -> {
      JobQueueTable jobQueue = JobQueueTable.getJobQueue(context, getCConf());
        // should be 0 jobs in the JobQueue to begin with
        Assert.assertEquals(0, getAllJobs(jobQueue, false).size());

        // Construct a partition notification with DATASET_ID
        Notification notification = Notification.forPartitions(DATASET_ID, ImmutableList.of());

        Assert.assertNull(jobQueue.getJob(SCHED1_JOB.getJobKey()));

        ProgramSchedule scheduleWithTimeout = new ProgramSchedule("SCHED1", "one partition schedule", WORKFLOW_ID,
                                                                  ImmutableMap.of("prop3", "abc"),
                                                                  new PartitionTrigger(DATASET_ID, 1),
                                                                  ImmutableList.of());

        Job jobWithTimeout = new SimpleJob(scheduleWithTimeout, 0,
                                           System.currentTimeMillis() - (Schedulers.JOB_QUEUE_TIMEOUT_MILLIS + 1),
                                           Lists.newArrayList(),
                                           Job.State.PENDING_TRIGGER, 0L);
        jobQueue.put(jobWithTimeout);
        Assert.assertEquals(jobWithTimeout, jobQueue.getJob(jobWithTimeout.getJobKey()));

        // before adding the notification, there should just be the job we added
        Assert.assertEquals(1, toSet(jobQueue.getJobsForSchedule(scheduleWithTimeout.getScheduleId()), true).size());

        // adding a notification will ignore the existing job (because it is timed out). It will create a new job
        // and add the notification to that new job
        jobQueue.addNotification(
          new ProgramScheduleRecord(SCHED1, new ProgramScheduleMeta(ProgramScheduleStatus.SCHEDULED, 0L)),
          notification);

        List<Job> jobs = new ArrayList<>(toSet(jobQueue.getJobsForSchedule(scheduleWithTimeout.getScheduleId()), true));
        // sort by creation time (oldest will be first in the list)
        Collections.sort(jobs, (o1, o2) -> Long.valueOf(o1.getCreationTime()).compareTo(o2.getCreationTime()));

        Assert.assertEquals(2, jobs.size());

        Job firstJob = jobs.get(0);
        // first job should have the same creation timestamp as the initially added job
        Assert.assertEquals(jobWithTimeout.getCreationTime(), firstJob.getCreationTime());
        // the notification we added shouldn't be in the first job
        Assert.assertEquals(0, firstJob.getNotifications().size());
        // first job should be marked to be deleted because it timed out
        Assert.assertTrue(firstJob.isToBeDeleted());
        // first job should have the same generation id as the timed out job
        Assert.assertEquals(jobWithTimeout.getGenerationId(), firstJob.getGenerationId());

        Job secondJob = jobs.get(1);
        // first job should not have the same creation timestamp as the initially added job
        Assert.assertNotEquals(jobWithTimeout.getCreationTime(), secondJob.getCreationTime());
        // the notification we added shouldn't be in the first job
        Assert.assertEquals(1, secondJob.getNotifications().size());
        Assert.assertEquals(notification, secondJob.getNotifications().get(0));
        // first job should not be marked to be deleted, since it was just created by our call to
        // JobQueue#addNotification
        Assert.assertFalse(secondJob.isToBeDeleted());
        // second job should have the next generation id
        Assert.assertEquals(jobWithTimeout.getGenerationId() + 1, secondJob.getGenerationId());
    });
  }

  private Set<Job> getAllJobs(JobQueueTable jobQueue) throws IOException {
    return getAllJobs(jobQueue, false);
  }

  private Set<Job> getAllJobs(JobQueueTable jobQueue, boolean includeToBeDeleted) throws IOException {
    Set<Job> jobs = new HashSet<>();
    for (int i = 0; i < jobQueue.getNumPartitions(); i++) {
      try (CloseableIterator<Job> allJobs = jobQueue.getJobs(i, null)) {
        jobs.addAll(toSet(allJobs, includeToBeDeleted));
      }
    }
    return jobs;
  }

  private Set<Job> toSet(CloseableIterator<Job> jobIter) {
    return toSet(jobIter, false);
  }

  private Set<Job> toSet(CloseableIterator<Job> jobIter, boolean includeToBeDeleted) {
    try {
      Set<Job> jobList = new HashSet<>();
      while (jobIter.hasNext()) {
        Job job = jobIter.next();
        if (includeToBeDeleted || !job.isToBeDeleted()) {
          jobList.add(job);
        }
      }
      return jobList;
    } finally {
      jobIter.close();
    }
  }
}
