/*
 * Copyright © 2026 Cask Data, Inc.
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Service;
import io.cdap.cdap.api.ProgramStatus;
import io.cdap.cdap.api.dataset.lib.AbstractCloseableIterator;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.features.Feature;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramSchedule;
import io.cdap.cdap.internal.app.runtime.schedule.constraint.ConcurrencyConstraint;
import io.cdap.cdap.internal.app.runtime.schedule.constraint.WorkflowTokenAvailableConstraint;
import io.cdap.cdap.internal.app.runtime.schedule.queue.Job;
import io.cdap.cdap.internal.app.runtime.schedule.queue.JobQueue;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.AndTrigger;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.OrTrigger;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.ProgramStatusTrigger;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.SatisfiableTrigger;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.TimeTrigger;
import io.cdap.cdap.internal.schedule.constraint.Constraint;
import io.cdap.cdap.proto.ProtoConstraint;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.WorkflowId;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link ConstraintCheckerService} constraint injection and trigger inspection.
 */
public class ConstraintCheckerServiceTest {

  private static final WorkflowId UPSTREAM_WORKFLOW =
      NamespaceId.DEFAULT.app("upstreamApp").workflow("DataPipelineWorkflow");
  private static final WorkflowId DOWNSTREAM_WORKFLOW =
      NamespaceId.DEFAULT.app("downstreamApp").workflow("DataPipelineWorkflow");

  @Test
  public void testContainsProgramStatusTriggerBareProgramStatus() {
    ProgramStatusTrigger trigger =
        new ProgramStatusTrigger(UPSTREAM_WORKFLOW, ProgramStatus.COMPLETED);
    Assert.assertTrue(ConstraintCheckerService.containsProgramStatusTrigger(trigger));
  }

  @Test
  public void testContainsProgramStatusTriggerBareTime() {
    TimeTrigger trigger = new TimeTrigger("0 * * * *");
    Assert.assertFalse(ConstraintCheckerService.containsProgramStatusTrigger(trigger));
  }

  @Test
  public void testContainsProgramStatusTriggerNull() {
    Assert.assertFalse(ConstraintCheckerService.containsProgramStatusTrigger(null));
  }

  @Test
  public void testContainsProgramStatusTriggerAndContaining() {
    AndTrigger trigger =
        new AndTrigger(
            new TimeTrigger("0 * * * *"),
            new ProgramStatusTrigger(UPSTREAM_WORKFLOW, ProgramStatus.COMPLETED));
    Assert.assertTrue(ConstraintCheckerService.containsProgramStatusTrigger(trigger));
  }

  @Test
  public void testContainsProgramStatusTriggerOrContaining() {
    OrTrigger trigger =
        new OrTrigger(
            new TimeTrigger("0 * * * *"),
            new ProgramStatusTrigger(UPSTREAM_WORKFLOW, ProgramStatus.FAILED));
    Assert.assertTrue(ConstraintCheckerService.containsProgramStatusTrigger(trigger));
  }

  @Test
  public void testContainsProgramStatusTriggerCompositeWithout() {
    AndTrigger trigger =
        new AndTrigger(new TimeTrigger("0 * * * *"), new TimeTrigger("0 0 * * *"));
    Assert.assertFalse(ConstraintCheckerService.containsProgramStatusTrigger(trigger));
  }

  @Test
  public void testContainsProgramStatusTriggerNestedCompositeContaining() {
    AndTrigger trigger =
        new AndTrigger(
            new TimeTrigger("0 * * * *"),
            new OrTrigger(
                new TimeTrigger("0 0 * * *"),
                new ProgramStatusTrigger(UPSTREAM_WORKFLOW, ProgramStatus.COMPLETED)));
    Assert.assertTrue(ConstraintCheckerService.containsProgramStatusTrigger(trigger));
  }

  @Test
  public void testContainsProgramStatusTriggerDeeplyNestedWithout() {
    OrTrigger trigger =
        new OrTrigger(
            new AndTrigger(new TimeTrigger("0 * * * *"), new TimeTrigger("0 0 * * *")),
            new TimeTrigger("0 12 * * *"));
    Assert.assertFalse(ConstraintCheckerService.containsProgramStatusTrigger(trigger));
  }

  @Test
  public void testEffectiveConstraintsDefaultFeatureFlagOff() {
    CConfiguration cConf = CConfiguration.create();
    ConstraintCheckerService service = createService(cConf);
    ProtoConstraint persisted = new ConcurrencyConstraint(2);
    Job job =
        jobWithTrigger(
            new ProgramStatusTrigger(UPSTREAM_WORKFLOW, ProgramStatus.COMPLETED),
            ImmutableList.of(persisted));

    List<Constraint> effective = Lists.newArrayList(service.getEffectiveConstraints(job));
    Assert.assertEquals(ImmutableList.<Constraint>of(persisted), effective);
  }

  @Test
  public void testEffectiveConstraintsInjectedWhenFeatureFlagEnabled() {
    CConfiguration cConf = CConfiguration.create();
    cConf.setBoolean("feature." + Feature.WORKFLOW_TOKEN_CONSTRAINT.getFeatureFlagString(), true);
    ConstraintCheckerService service = createService(cConf);
    ProtoConstraint persisted = new ConcurrencyConstraint(2);
    Job job =
        jobWithTrigger(
            new ProgramStatusTrigger(UPSTREAM_WORKFLOW, ProgramStatus.COMPLETED),
            ImmutableList.of(persisted));

    List<Constraint> effective = Lists.newArrayList(service.getEffectiveConstraints(job));
    Assert.assertEquals(2, effective.size());
    Assert.assertSame(persisted, effective.get(0));
    Assert.assertTrue(effective.get(1) instanceof WorkflowTokenAvailableConstraint);
  }

  @Test
  public void testEffectiveConstraintsInjectedForCompositeTriggerWhenFlagEnabled() {
    CConfiguration cConf = CConfiguration.create();
    cConf.setBoolean("feature." + Feature.WORKFLOW_TOKEN_CONSTRAINT.getFeatureFlagString(), true);
    ConstraintCheckerService service = createService(cConf);
    Job job =
        jobWithTrigger(
            new AndTrigger(
                new TimeTrigger("0 * * * *"),
                new ProgramStatusTrigger(UPSTREAM_WORKFLOW, ProgramStatus.COMPLETED)),
            Collections.<ProtoConstraint>emptyList());

    List<Constraint> effective = Lists.newArrayList(service.getEffectiveConstraints(job));
    Assert.assertEquals(1, effective.size());
    Assert.assertTrue(effective.get(0) instanceof WorkflowTokenAvailableConstraint);
  }

  @Test
  public void testEffectiveConstraintsNotInjectedForTimeTriggerWhenFlagEnabled() {
    CConfiguration cConf = CConfiguration.create();
    cConf.setBoolean("feature." + Feature.WORKFLOW_TOKEN_CONSTRAINT.getFeatureFlagString(), true);
    ConstraintCheckerService service = createService(cConf);
    ProtoConstraint persisted = new ConcurrencyConstraint(1);
    Job job = jobWithTrigger(new TimeTrigger("0 * * * *"), ImmutableList.of(persisted));

    List<Constraint> effective = Lists.newArrayList(service.getEffectiveConstraints(job));
    Assert.assertEquals(ImmutableList.<Constraint>of(persisted), effective);
  }

  @Test
  public void testStartUpShutDownAndStopwatchConstraintScan() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.setInt(Constants.Scheduler.JOB_QUEUE_NUM_PARTITIONS, 0);
    ConstraintCheckerService service = createService(cConf);

    service.startAsync().awaitRunning();
    Assert.assertEquals(Service.State.RUNNING, service.state());

    Class<?> threadClass =
        Class.forName("io.cdap.cdap.scheduler.ConstraintCheckerService$ConstraintCheckerThread");
    Constructor<?> ctor =
        threadClass.getDeclaredConstructor(ConstraintCheckerService.class, int.class);
    ctor.setAccessible(true);
    Object checkerThread = ctor.newInstance(service, 0);

    Method checkMethod = threadClass.getDeclaredMethod("checkJobConstraints", JobQueue.class);
    checkMethod.setAccessible(true);

    // 1. Non-empty scan with satisfied PENDING_CONSTRAINT job and timed-out job.
    long now = System.currentTimeMillis();
    Job readyJob =
        jobWithTrigger(new TimeTrigger("0 * * * *"), Collections.<ProtoConstraint>emptyList());
    Mockito.when(readyJob.getCreationTime()).thenReturn(now);
    Mockito.when(readyJob.getState()).thenReturn(Job.State.PENDING_CONSTRAINT);

    Job timedOutJob =
        jobWithTrigger(new TimeTrigger("0 * * * *"), Collections.<ProtoConstraint>emptyList());
    Mockito.when(timedOutJob.getCreationTime()).thenReturn(0L);
    Mockito.when(timedOutJob.getState()).thenReturn(Job.State.PENDING_CONSTRAINT);

    AtomicBoolean iterClosed = new AtomicBoolean();
    JobQueue jobQueue = Mockito.mock(JobQueue.class);
    Mockito.when(jobQueue.getJobs(Mockito.eq(0), Mockito.any()))
        .thenReturn(closeableIter(ImmutableList.of(readyJob, timedOutJob).iterator(), iterClosed));

    boolean emptyScan = (Boolean) checkMethod.invoke(checkerThread, jobQueue);
    Assert.assertFalse(emptyScan);
    Assert.assertTrue(iterClosed.get());
    Mockito.verify(jobQueue).transitState(readyJob, Job.State.PENDING_LAUNCH);
    Mockito.verify(jobQueue).deleteJob(timedOutJob);

    // 2. Empty queue scan returns true.
    AtomicBoolean emptyClosed = new AtomicBoolean();
    JobQueue emptyQueue = Mockito.mock(JobQueue.class);
    Mockito.when(emptyQueue.getJobs(Mockito.eq(0), Mockito.any()))
        .thenReturn(closeableIter(Collections.<Job>emptyIterator(), emptyClosed));
    boolean isEmpty = (Boolean) checkMethod.invoke(checkerThread, emptyQueue);
    Assert.assertTrue(isEmpty);
    Assert.assertTrue(emptyClosed.get());

    service.stopAsync().awaitTerminated();
    Assert.assertEquals(Service.State.TERMINATED, service.state());
  }

  private static CloseableIterator<Job> closeableIter(
      Iterator<Job> delegate, AtomicBoolean closed) {
    return new AbstractCloseableIterator<Job>() {
      @Override
      protected Job computeNext() {
        return delegate.hasNext() ? delegate.next() : endOfData();
      }

      @Override
      public void close() {
        closed.set(true);
      }
    };
  }

  private static ConstraintCheckerService createService(CConfiguration cConf) {
    return new ConstraintCheckerService(
        null, null, null, null, cConf, null, Mockito.mock(MetricsCollectionService.class));
  }

  private static Job jobWithTrigger(
      SatisfiableTrigger trigger, List<ProtoConstraint> constraints) {
    ProgramSchedule schedule =
        new ProgramSchedule(
            "sched1",
            "desc",
            DOWNSTREAM_WORKFLOW,
            Collections.<String, String>emptyMap(),
            trigger,
            constraints);
    Job job = Mockito.mock(Job.class);
    Mockito.when(job.getSchedule()).thenReturn(schedule);
    return job;
  }
}
