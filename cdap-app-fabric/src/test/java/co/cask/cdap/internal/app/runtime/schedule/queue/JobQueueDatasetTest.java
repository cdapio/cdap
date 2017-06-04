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

package co.cask.cdap.internal.app.runtime.schedule.queue;

import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.data.runtime.DynamicTransactionExecutorFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.TransactionExecutorFactory;
import co.cask.cdap.internal.app.runtime.schedule.ProgramSchedule;
import co.cask.cdap.internal.app.runtime.schedule.ProgramScheduleMeta;
import co.cask.cdap.internal.app.runtime.schedule.ProgramScheduleRecord;
import co.cask.cdap.internal.app.runtime.schedule.ProgramScheduleStatus;
import co.cask.cdap.internal.app.runtime.schedule.store.Schedulers;
import co.cask.cdap.internal.app.runtime.schedule.trigger.PartitionTrigger;
import co.cask.cdap.internal.app.runtime.schedule.trigger.TimeTrigger;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.internal.schedule.constraint.Constraint;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.WorkflowId;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionSystemClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link JobQueueDataset}.
 */
public class JobQueueDatasetTest extends AppFabricTestBase {
  private static final NamespaceId TEST_NS = new NamespaceId("jobQueueTest");
  private static final ApplicationId APP_ID = TEST_NS.app("app1");
  private static final WorkflowId WORKFLOW_ID = APP_ID.workflow("wf1");
  private static final DatasetId DATASET_ID = TEST_NS.dataset("pfs1");

  private static final ProgramSchedule SCHED1 = new ProgramSchedule("SCHED1", "one partition schedule", WORKFLOW_ID,
                                                                    ImmutableMap.of("prop3", "abc"),
                                                                    new PartitionTrigger(DATASET_ID, 1),
                                                                    ImmutableList.<Constraint>of());
  private static final ProgramSchedule SCHED2 = new ProgramSchedule("SCHED2", "time schedule", WORKFLOW_ID,
                                                                    ImmutableMap.of("prop3", "abc"),
                                                                    new TimeTrigger("* * * * *"),
                                                                    ImmutableList.<Constraint>of());

  private static final Job SCHED1_JOB = new SimpleJob(SCHED1, System.currentTimeMillis(),
                                                      Lists.<Notification>newArrayList(),
                                                      Job.State.PENDING_TRIGGER, 0L);
  private static final Job SCHED2_JOB = new SimpleJob(SCHED2, System.currentTimeMillis(),
                                                      Lists.<Notification>newArrayList(),
                                                      Job.State.PENDING_TRIGGER, 0L);

  private TransactionExecutor txExecutor;
  private JobQueueDataset jobQueue;

  @Before
  public void before() throws Exception {
    DatasetFramework dsFramework = getInjector().getInstance(DatasetFramework.class);
    TransactionSystemClient txClient = getInjector().getInstance(TransactionSystemClient.class);
    TransactionExecutorFactory txExecutorFactory = new DynamicTransactionExecutorFactory(txClient);
    jobQueue = dsFramework.getDataset(Schedulers.JOB_QUEUE_DATASET_ID, new HashMap<String, String>(), null);
    Assert.assertNotNull(jobQueue);
    this.txExecutor =
      txExecutorFactory.createExecutor(Collections.singleton((TransactionAware) jobQueue));
  }

  @After
  public void tearDown() throws Exception {
    txExecutor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        for (Job job : getAllJobs(jobQueue, true)) {
          jobQueue.deleteJob(job);
        }
      }
    });
  }

  @Test
  public void checkDatasetType() throws DatasetManagementException {
    DatasetFramework dsFramework = getInjector().getInstance(DatasetFramework.class);
    Assert.assertTrue(dsFramework.hasType(NamespaceId.SYSTEM.datasetType(JobQueueDataset.class.getName())));
  }

  @Test
  public void testMessageId() throws Exception {
    final String topic1 = "topic1";
    final String topic2 = "topic2";
    final String messageIdToPut = "messageIdToPut";

    txExecutor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // without first setting the message Id, a get will return null
        Assert.assertNull(jobQueue.retrieveSubscriberState(topic1));

        // test set and get
        jobQueue.persistSubscriberState(topic1, messageIdToPut);
        Assert.assertEquals(messageIdToPut, jobQueue.retrieveSubscriberState(topic1));

        // the message id for a different topic should still be null
        Assert.assertNull(jobQueue.retrieveSubscriberState(topic2));
      }
    });

    txExecutor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // the message Id should be retrievable across transactions
        Assert.assertEquals(messageIdToPut, jobQueue.retrieveSubscriberState(topic1));
        Assert.assertNull(jobQueue.retrieveSubscriberState(topic2));
      }
    });
  }

  @Test
  public void testJobQueue() throws Exception {
    txExecutor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
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
      }
    });

    // doing the last assertion in a separate transaction to workaround CDAP-3511
    txExecutor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Assert.assertEquals(ImmutableSet.of(SCHED2_JOB), getAllJobs(jobQueue));
        Assert.assertEquals(0, toSet(jobQueue.getJobsForSchedule(SCHED1.getScheduleId())).size());
        Assert.assertEquals(ImmutableSet.of(SCHED2_JOB), toSet(jobQueue.getJobsForSchedule(SCHED2.getScheduleId())));
      }
    });

    // now actually delete all jobs that are marked for deletion
    txExecutor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        for (Job job : getAllJobs(jobQueue, true)) {
          if (job.isToBeDeleted()) {
            jobQueue.deleteJob(job);
          }
        }
      }
    });

    // and validate that the job is completely gone
    txExecutor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Assert.assertEquals(ImmutableSet.of(SCHED2_JOB), getAllJobs(jobQueue, true));
        Assert.assertEquals(0, toSet(jobQueue.getJobsForSchedule(SCHED1.getScheduleId())).size());
        Assert.assertEquals(ImmutableSet.of(SCHED2_JOB), toSet(jobQueue.getJobsForSchedule(SCHED2.getScheduleId())));
      }
    });
  }

  @Test
  public void testGetAllJobs() throws Exception {
    // Test that getJobs can be called even when there are messageIds persisted in the same dataset.
    // This tests rowkey isolation, since getJobs is a scan across the entire job row-space.
    txExecutor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // should be 0 jobs in the JobQueue to begin with
        Assert.assertEquals(0, getAllJobs(jobQueue).size());

        jobQueue.persistSubscriberState("someTopic", "someMessageId");

        // put a job for SCHED1, and check that it is in 'getJobs' and 'getJobsForSchedule'
        jobQueue.put(SCHED1_JOB);

        Assert.assertEquals(ImmutableSet.of(SCHED1_JOB), getAllJobs(jobQueue));
        Assert.assertEquals(ImmutableSet.of(SCHED1_JOB), toSet(jobQueue.fullScan()));
      }
    });
  }


  @Test
  public void testJobQueueIteration() throws Exception {
    // Test that getJobs can be leveraged to iterate over the JobQueue partially (and across transactions).
    // The Job last consumed from the Iterator returned in the first iteration can be passed to the next call to resume
    // scanning from
    txExecutor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // should be 0 jobs in the JobQueue to begin with
        Assert.assertEquals(0, getAllJobs(jobQueue).size());

        Multimap<Integer, Job> jobsByPartition = HashMultimap.create();
        long now = 1494353984967L;
        for (int i = 0; i < 100; i++) {
          ProgramSchedule schedule = new ProgramSchedule("sched" + i, "one partition schedule", WORKFLOW_ID,
                                                         ImmutableMap.<String, String>of(),
                                                         new PartitionTrigger(DATASET_ID, 1),
                                                         ImmutableList.<Constraint>of());
          Job job = new SimpleJob(schedule, now + i, ImmutableList.<Notification>of(),
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
      }
    });
  }

  @Test
  public void testAddNotifications() throws Exception {
    txExecutor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // should be 0 jobs in the JobQueue to begin with
        Assert.assertEquals(0, getAllJobs(jobQueue, false).size());

        // Construct a partition notification with DATASET_ID
        Notification notification = Notification.forPartitions(DATASET_ID, ImmutableList.<PartitionKey>of());

        Assert.assertNull(jobQueue.getJob(SCHED1_JOB.getJobKey()));

        jobQueue.put(SCHED1_JOB);
        Assert.assertEquals(SCHED1_JOB, jobQueue.getJob(SCHED1_JOB.getJobKey()));

        // Since notification and SCHED1 have the same dataset id DATASET_ID, notification will be added to
        // SCHED1_JOB, which is a job in SCHED1
        jobQueue.addNotification(
          new ProgramScheduleRecord(SCHED1, new ProgramScheduleMeta(ProgramScheduleStatus.SCHEDULED, 0L)),
          notification);
        Assert.assertEquals(ImmutableList.of(notification), jobQueue.getJob(SCHED1_JOB.getJobKey()).getNotifications());
      }
    });
  }

  @Test
  public void testJobTimeout() throws Exception {
    txExecutor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // should be 0 jobs in the JobQueue to begin with
        Assert.assertEquals(0, getAllJobs(jobQueue, false).size());

        // Construct a partition notification with DATASET_ID
        Notification notification = Notification.forPartitions(DATASET_ID, ImmutableList.<PartitionKey>of());

        Assert.assertNull(jobQueue.getJob(SCHED1_JOB.getJobKey()));

        ProgramSchedule scheduleWithTimeout = new ProgramSchedule("SCHED1", "one partition schedule", WORKFLOW_ID,
                                                                  ImmutableMap.of("prop3", "abc"),
                                                                  new PartitionTrigger(DATASET_ID, 1),
                                                                  ImmutableList.<Constraint>of());

        Job jobWithTimeout = new SimpleJob(scheduleWithTimeout,
                                           System.currentTimeMillis() - Schedulers.JOB_QUEUE_TIMEOUT_MILLIS,
                                           Lists.<Notification>newArrayList(),
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
        Collections.sort(jobs, new Comparator<Job>() {
          @Override
          public int compare(Job o1, Job o2) {
            return Long.valueOf(o1.getCreationTime()).compareTo(o2.getCreationTime());
          }
        });

        Assert.assertEquals(2, jobs.size());

        Job firstJob = jobs.get(0);
        // first job should have the same creation timestamp as the initially added job
        Assert.assertEquals(jobWithTimeout.getCreationTime(), firstJob.getCreationTime());
        // the notification we added shouldn't be in the first job
        Assert.assertEquals(0, firstJob.getNotifications().size());
        // first job should be marked to be deleted because it timed out
        Assert.assertTrue(firstJob.isToBeDeleted());

        Job secondJob = jobs.get(1);
        // first job should not have the same creation timestamp as the initially added job
        Assert.assertNotEquals(jobWithTimeout.getCreationTime(), secondJob.getCreationTime());
        // the notification we added shouldn't be in the first job
        Assert.assertEquals(1, secondJob.getNotifications().size());
        Assert.assertEquals(notification, secondJob.getNotifications().get(0));
        // first job should not be marked to be deleted, since it was just created by our call to
        // JobQueue#addNotification
        Assert.assertFalse(secondJob.isToBeDeleted());
      }
    });
  }

  private Set<Job> getAllJobs(JobQueueDataset jobQueue) {
    return getAllJobs(jobQueue, false);
  }

  private Set<Job> getAllJobs(JobQueueDataset jobQueue, boolean includeToBeDeleted) {
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
