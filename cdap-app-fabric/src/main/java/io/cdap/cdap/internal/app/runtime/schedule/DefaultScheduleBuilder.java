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

package io.cdap.cdap.internal.app.runtime.schedule;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.ProgramStatus;
import io.cdap.cdap.api.app.ProgramType;
import io.cdap.cdap.api.schedule.ConstraintProgramScheduleBuilder;
import io.cdap.cdap.api.schedule.ScheduleBuilder;
import io.cdap.cdap.api.schedule.Trigger;
import io.cdap.cdap.api.schedule.TriggerFactory;
import io.cdap.cdap.internal.app.runtime.schedule.constraint.ConcurrencyConstraint;
import io.cdap.cdap.internal.app.runtime.schedule.constraint.DelayConstraint;
import io.cdap.cdap.internal.app.runtime.schedule.constraint.LastRunConstraint;
import io.cdap.cdap.internal.app.runtime.schedule.constraint.TimeRangeConstraint;
import io.cdap.cdap.internal.app.runtime.schedule.store.Schedulers;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.TriggerBuilder;
import io.cdap.cdap.internal.schedule.ScheduleCreationSpec;
import io.cdap.cdap.internal.schedule.constraint.Constraint;
import io.cdap.cdap.proto.ProtoConstraint;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * The default implementation of {@link ScheduleBuilder}.
 */
public class DefaultScheduleBuilder implements ConstraintProgramScheduleBuilder {

  private final String name;
  private final TriggerFactory triggerFactory;
  private final String programName;
  private final List<ProtoConstraint> constraints;
  private String description;
  private Map<String, String> properties;
  private long timeoutMillis = Schedulers.JOB_QUEUE_TIMEOUT_MILLIS;

  public DefaultScheduleBuilder(String name, String programName, TriggerFactory triggerFactory) {
    this.name = name;
    this.description = "";
    this.programName = programName;
    this.triggerFactory = triggerFactory;
    this.properties = new HashMap<>();
    this.constraints = new ArrayList<>();
  }

  @Override
  public ScheduleBuilder setDescription(String description) {
    this.description = description;
    return this;
  }

  @Override
  public ScheduleBuilder setProperties(Map<String, String> properties) {
    this.properties = ImmutableMap.copyOf(properties);
    return this;
  }

  @Override
  public ScheduleBuilder setTimeout(long time, TimeUnit unit) {
    this.timeoutMillis = unit.toMillis(time);
    return this;
  }

  @Override
  public ConstraintProgramScheduleBuilder withConcurrency(int max) {
    constraints.add(new ConcurrencyConstraint(max));
    return this;
  }

  @Override
  public ScheduleBuilder withDelay(long delay, TimeUnit timeUnit) {
    constraints.add(new DelayConstraint(delay, timeUnit));
    return this;
  }

  @Override
  public ConstraintProgramScheduleBuilder withTimeWindow(String startTime, String endTime) {
    constraints.add(new TimeRangeConstraint(startTime, endTime, TimeZone.getDefault()));
    return this;
  }

  @Override
  public ConstraintProgramScheduleBuilder withTimeWindow(String startTime, String endTime, TimeZone timeZone) {
    constraints.add(new TimeRangeConstraint(startTime, endTime, timeZone));
    return this;
  }

  @Override
  public ConstraintProgramScheduleBuilder withDurationSinceLastRun(long duration, TimeUnit unit) {
    constraints.add(new LastRunConstraint(duration, unit));
    return this;
  }

  @Override
  public ScheduleCreationSpec triggerByTime(String cronExpression) {
    return triggerOn(triggerFactory.byTime(cronExpression));
  }

  @Override
  public ScheduleCreationSpec triggerOnPartitions(String datasetName, int numPartitions) {
    return triggerOn(triggerFactory.onPartitions(datasetName, numPartitions));
  }

  @Override
  public ScheduleCreationSpec triggerOnPartitions(String datasetNamespace, String datasetName,
                                                  int numPartitions) {
    return triggerOn(triggerFactory.onPartitions(datasetNamespace, datasetName, numPartitions));
  }

  @Override
  public ScheduleCreationSpec triggerOnProgramStatus(String programNamespace, String application,
                                                     String appVersion, ProgramType programType, String program,
                                                     ProgramStatus... programStatuses) {
    return triggerOn(triggerFactory.onProgramStatus(programNamespace, application, appVersion,
                                                    programType, program, programStatuses));
  }

  @Override
  public ScheduleCreationSpec triggerOnProgramStatus(String programNamespace, String application,
                                                     ProgramType programType, String program,
                                                     ProgramStatus... programStatuses) {
    return triggerOn(triggerFactory.onProgramStatus(programNamespace, application,
                                                    programType, program, programStatuses));
  }

  @Override
  public ScheduleCreationSpec triggerOnProgramStatus(String application, ProgramType programType, String program,
                                                     ProgramStatus... programStatuses) {
    return triggerOn(triggerFactory.onProgramStatus(application, programType, program, programStatuses));
  }

  @Override
  public ScheduleCreationSpec triggerOnProgramStatus(ProgramType programType, String program,
                                                     ProgramStatus... programStatuses) {
    return triggerOn(triggerFactory.onProgramStatus(programType, program, programStatuses));
  }

  @Override
  public ScheduleCreationSpec triggerOn(Trigger trigger) {
    if (trigger instanceof TriggerBuilder) {
      return new ScheduleCreationBuilder(name, description, programName, properties, constraints, timeoutMillis,
                                         (TriggerBuilder) trigger);
    }
    return new ScheduleCreationSpec(name, description, programName, properties, trigger, constraints, timeoutMillis);
  }

  @Override
  public ScheduleBuilder waitUntilMet() {
    // user will only be able to call waitUntilMet right after they add a Constraint
    constraints.get(constraints.size() - 1).setWaitUntilMet(true);
    return this;
  }

  @Override
  public ScheduleBuilder abortIfNotMet() {
    // user will only be able to call abortIfNotMet right after they add a Constraint
    constraints.get(constraints.size() - 1).setWaitUntilMet(false);
    return this;
  }

  /**
   * Inner class that creates a ScheduleCreationSpec from the deployed properties
   */
  public class ScheduleCreationBuilder extends ScheduleCreationSpec {
    private final TriggerBuilder triggerBuilder;

    private ScheduleCreationBuilder(String name, String description, String programName, Map<String, String> properties,
                                   List<? extends Constraint> constraints, long timeoutMillis,
                                   TriggerBuilder triggerBuilder) {
      super(name, description, programName, properties, null, constraints, timeoutMillis);
      this.triggerBuilder = triggerBuilder;
    }

    @Override
    public Trigger getTrigger() {
      throw new UnsupportedOperationException(String.format("Schedule %s does not have a trigger because it is  " +
                                                            "missing a defined namespace and application environment",
                                                            getName()));
    }

    public ScheduleCreationSpec build(String namespace, String applicationName, String applicationVersion) {
      return new ScheduleCreationSpec(getName(), getDescription(), getProgramName(), getProperties(),
                                      triggerBuilder.build(namespace, applicationName, applicationVersion),
                                      getConstraints(), getTimeoutMillis());
    }
  }
}
