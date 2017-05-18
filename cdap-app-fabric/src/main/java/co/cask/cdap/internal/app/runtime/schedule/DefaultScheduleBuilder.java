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

package co.cask.cdap.internal.app.runtime.schedule;

import co.cask.cdap.api.schedule.ConstraintProgramScheduleBuilder;
import co.cask.cdap.api.schedule.ScheduleBuilder;
import co.cask.cdap.internal.app.runtime.schedule.constraint.ConcurrencyConstraint;
import co.cask.cdap.internal.app.runtime.schedule.constraint.DelayConstraint;
import co.cask.cdap.internal.app.runtime.schedule.constraint.LastRunConstraint;
import co.cask.cdap.internal.app.runtime.schedule.constraint.TimeRangeConstraint;
import co.cask.cdap.internal.app.runtime.schedule.trigger.PartitionTrigger;
import co.cask.cdap.internal.app.runtime.schedule.trigger.TimeTrigger;
import co.cask.cdap.internal.schedule.ScheduleCreationSpec;
import co.cask.cdap.proto.ProtoConstraint;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * The default implementation of {@link ScheduleBuilder}.
 */
public class DefaultScheduleBuilder implements ConstraintProgramScheduleBuilder {

  private final String name;
  private final NamespaceId namespace;
  private final String programName;
  private final List<ProtoConstraint> constraints;
  private String description;
  private Map<String, String> properties;

  public DefaultScheduleBuilder(String name, NamespaceId namespace, String programName) {
    this.name = name;
    this.description = "";
    this.namespace = namespace;
    this.programName = programName;
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
  public ConstraintProgramScheduleBuilder withConcurrency(int max) {
    constraints.add(new ConcurrencyConstraint(max));
    return this;
  }

  @Override
  public ScheduleBuilder withDelay(long delayMillis) {
    constraints.add(new DelayConstraint(delayMillis));
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
  public ConstraintProgramScheduleBuilder withDurationSinceLastRun(long delayMillis) {
    constraints.add(new LastRunConstraint(delayMillis));
    return this;
  }

  @Override
  public ScheduleCreationSpec triggerByTime(String cronExpression) {
    return new ScheduleCreationSpec(name, description, programName, properties,
                                    new TimeTrigger(cronExpression), constraints);
  }

  @Override
  public ScheduleCreationSpec triggerOnPartitions(String datasetName, int numPartitions) {
    return new ScheduleCreationSpec(name, description, programName, properties,
                                    new PartitionTrigger(namespace.dataset(datasetName), numPartitions),
                                    constraints);
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
}
