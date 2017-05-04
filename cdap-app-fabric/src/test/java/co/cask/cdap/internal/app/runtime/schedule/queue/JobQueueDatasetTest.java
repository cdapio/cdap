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
import co.cask.cdap.data.runtime.DynamicTransactionExecutorFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.TransactionExecutorFactory;
import co.cask.cdap.internal.app.runtime.schedule.ProgramSchedule;
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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

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
                                                      Job.State.PENDING_TRIGGER);
  private static final Job SCHED2_JOB = new SimpleJob(SCHED2, System.currentTimeMillis(),
                                                      Lists.<Notification>newArrayList(),
                                                      Job.State.PENDING_TRIGGER);

  private TransactionExecutor txExecutor;
  private JobQueueDataset jobQueue;

  @Before
  public void before() throws Exception {
    DatasetFramework dsFramework = getInjector().getInstance(DatasetFramework.class);
    TransactionSystemClient txClient = getInjector().getInstance(TransactionSystemClient.class);
    TransactionExecutorFactory txExecutorFactory = new DynamicTransactionExecutorFactory(txClient);
    jobQueue = dsFramework.getDataset(Schedulers.JOB_QUEUE_DATASET_ID,
                                                            new HashMap<String, String>(), null);
    Assert.assertNotNull(jobQueue);
    this.txExecutor =
      txExecutorFactory.createExecutor(Collections.singleton((TransactionAware) jobQueue));
  }

  @After
  public void tearDown() throws Exception {
    txExecutor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        for (Job job : getAllJobs(jobQueue)) {
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

        // delete job for SCHED1 and assert that it is no longer in 'getJobs'
        jobQueue.deleteJobs(SCHED1.getScheduleId());
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
                                  Job.State.PENDING_TRIGGER);
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
        Assert.assertEquals(0, getAllJobs(jobQueue).size());

        Notification notification =
          new Notification(Notification.Type.PARTITION, ImmutableMap.of("someKey", "someValue"));

        Assert.assertNull(jobQueue.getJob(SCHED1_JOB.getJobKey()));

        jobQueue.put(SCHED1_JOB);
        Assert.assertEquals(SCHED1_JOB, jobQueue.getJob(SCHED1_JOB.getJobKey()));

        jobQueue.addNotification(SCHED1, notification);
        Assert.assertEquals(ImmutableList.of(notification), jobQueue.getJob(SCHED1_JOB.getJobKey()).getNotifications());
      }
    });
  }

  private Set<Job> getAllJobs(JobQueueDataset jobQueue) {
    Set<Job> jobs = new HashSet<>();
    for (int i = 0; i < jobQueue.getNumPartitions(); i++) {
      try (CloseableIterator<Job> allJobs = jobQueue.getJobs(i, null)) {
        jobs.addAll(toSet(allJobs));
      }
    }
    return jobs;
  }

  private Set<Job> toSet(CloseableIterator<Job> jobIter) {
    try {
      Set<Job> jobList = new HashSet<>();
      while (jobIter.hasNext()) {
        jobList.add(jobIter.next());
      }
      return jobList;
    } finally {
      jobIter.close();
    }
  }
}
