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

import co.cask.cdap.api.schedule.ScheduleConfigurer;
import co.cask.cdap.internal.app.runtime.schedule.constraint.ConcurrencyConstraint;
import co.cask.cdap.internal.app.runtime.schedule.constraint.DelayConstraint;
import co.cask.cdap.internal.app.runtime.schedule.constraint.DurationSinceLastRunConstraint;
import co.cask.cdap.internal.app.runtime.schedule.constraint.TimeRangeConstraint;
import co.cask.cdap.internal.app.runtime.schedule.trigger.PartitionTrigger;
import co.cask.cdap.internal.app.runtime.schedule.trigger.TimeTrigger;
import co.cask.cdap.internal.schedule.ScheduleCreationSpec;
import co.cask.cdap.internal.schedule.constraint.Constraint;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * The default implementation of {@link ScheduleConfigurer}.
 */
public class DefaultScheduleConfigurer implements ScheduleConfigurer {

  private final String name;
  private final Map<String, ScheduleCreationSpec> programSchedules;
  private final NamespaceId namespace;
  private final String programName;
  private final List<Constraint> constraints;
  private String description;
  private Map<String, String> properties;

  public DefaultScheduleConfigurer(String name, NamespaceId namespace, String programName,
                                   Map<String, ScheduleCreationSpec> programSchedules) {
    this.name = name;
    this.description = "";
    this.namespace = namespace;
    this.programName = programName;
    this.properties = new HashMap<>();
    this.constraints = new ArrayList<>();

    this.programSchedules = programSchedules;
  }

  @Override
  public ScheduleConfigurer setDescription(String description) {
    this.description = description;
    return this;
  }

  @Override
  public ScheduleConfigurer setProperties(Map<String, String> properties) {
    this.properties = ImmutableMap.copyOf(properties);
    return this;
  }

  @Override
  public ScheduleConfigurer limitConcurrentRuns(int max) {
    if (max < 1) {
      throw new IllegalArgumentException("max concurrent runs must be at least 1.");
    }
    constraints.add(new ConcurrencyConstraint(max));
    return this;
  }

  @Override
  public ScheduleConfigurer delayRun(long delayMillis) {
    // TODO: disallow from being called multiple times?
    constraints.add(new DelayConstraint(delayMillis));
    return this;
  }

  @Override
  public ScheduleConfigurer setTimeRange(String startTime, String endTime) {
    constraints.add(new TimeRangeConstraint(startTime, endTime, TimeZone.getDefault()));
    return this;
  }

  @Override
  public ScheduleConfigurer setTimeRange(String startTime, String endTime, TimeZone timeZone) {
    constraints.add(new TimeRangeConstraint(startTime, endTime, timeZone));
    return this;
  }

  @Override
  public ScheduleConfigurer setDurationSinceLastRun(long delayMillis) {
    constraints.add(new DurationSinceLastRunConstraint(delayMillis));
    return this;
  }

  @Override
  public void triggerByTime(String cronExpression) {
    setSchedule(new ScheduleCreationSpec(name, description, programName, properties,
                                         new TimeTrigger(cronExpression), constraints));
  }

  @Override
  public void triggerOnPartitions(String datasetName, int numPartitions) {
    setSchedule(new ScheduleCreationSpec(name, description, programName, properties,
                                         new PartitionTrigger(namespace.dataset(datasetName), numPartitions),
                                         constraints));
  }

  private void setSchedule(ScheduleCreationSpec schedule) {
    // setSchedule can not be called twice on the same configurer (semantics are not defined)
    Preconditions.checkArgument(null == programSchedules.put(schedule.getName(), schedule),
                                "Duplicate schedule name for schedule: '%s'", schedule.getName());
  }
}
