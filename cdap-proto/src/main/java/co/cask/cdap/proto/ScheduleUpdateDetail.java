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

package co.cask.cdap.proto;

import co.cask.cdap.api.schedule.RunConstraints;
import co.cask.cdap.api.schedule.ScheduleSpecification;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * A class which wraps fields which can be updated for an existing {@link ScheduleSpecification}
 */
public class ScheduleUpdateDetail {

  private final Schedule schedule;
  private final Map<String, String> properties;

  public ScheduleUpdateDetail(String scheduleDescription, RunConstraints runConstraints, String cronExpression,
                              String streamName, Integer dataTriggerMB, Map<String, String> properties) {
    this.schedule = new Schedule(scheduleDescription, runConstraints, cronExpression, streamName, dataTriggerMB);
    this.properties = properties;
  }

  /**
   * Returns the schedule configuration details provided by the user for update. This will be null if the user didn't
   * provide any configuration to update the schedule but is just updating the properties
   */
  @Nullable
  public Schedule getSchedule() {
    return schedule;
  }

  @Nullable
  public Map<String, String> getProperties() {
    return properties;
  }

  /**
   * A wrapper around schedule configuration which can be updated. This is a inner class of
   * {@link ScheduleUpdateDetail} so that we can have a similar json structure with
   * {@link ScheduleSpecification} except the configurations which cannot be updated like schedule type
   */
  public class Schedule {
    private final String description;
    private final RunConstraints runConstraints;
    private final String cronExpression;
    private final String streamName;
    private final Integer dataTriggerMB;

    Schedule(String description, RunConstraints runConstraints, String cronExpression,
             String streamName, Integer dataTriggerMB) {
      this.description = description;
      this.runConstraints = runConstraints;
      this.cronExpression = cronExpression;
      this.streamName = streamName;
      this.dataTriggerMB = dataTriggerMB;
    }

    @Nullable
    public String getDescription() {
      return description;
    }

    public RunConstraints getRunConstraints() {
      return runConstraints;
    }

    @Nullable
    public String getCronExpression() {
      return cronExpression;
    }

    @Nullable
    public String getStreamName() {
      return streamName;
    }

    @Nullable
    public Integer getDataTriggerMB() {
      return dataTriggerMB;
    }
  }
}
