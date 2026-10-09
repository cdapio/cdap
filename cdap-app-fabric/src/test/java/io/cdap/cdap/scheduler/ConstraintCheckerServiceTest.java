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
import io.cdap.cdap.api.ProgramStatus;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.features.Feature;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramSchedule;
import io.cdap.cdap.internal.app.runtime.schedule.constraint.ConcurrencyConstraint;
import io.cdap.cdap.internal.app.runtime.schedule.constraint.WorkflowTokenAvailableConstraint;
import io.cdap.cdap.internal.app.runtime.schedule.queue.Job;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.AndTrigger;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.OrTrigger;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.ProgramStatusTrigger;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.SatisfiableTrigger;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.TimeTrigger;
import io.cdap.cdap.internal.schedule.constraint.Constraint;
import io.cdap.cdap.proto.ProtoConstraint;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.WorkflowId;
import java.util.Collections;
import java.util.List;
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

  private static ConstraintCheckerService createService(CConfiguration cConf) {
    return new ConstraintCheckerService(null, null, null, null, cConf, null, null);
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
