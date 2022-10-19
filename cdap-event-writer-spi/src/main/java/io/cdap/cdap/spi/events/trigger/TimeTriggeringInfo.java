/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.spi.events.trigger;

import java.util.Map;
import java.util.Objects;

/**
 * Represents the properties of a Time based Trigger.
 */
public class TimeTriggeringInfo implements TriggeringInfo {
  private final Type type;
  private final ScheduleId scheduleId;
  private final String cronExpression;
  private final Map<String, String> properties;

  public TimeTriggeringInfo(ScheduleId scheduleId, String cronExpression, Map<String, String> properties) {
    this.type = Type.TIME;
    this.scheduleId = scheduleId;
    this.cronExpression = cronExpression;
    this.properties = properties;
  }

  /**
   * @return The type of the trigger
   */
  @Override
  public Type getType() {
    return type;
  }

  /**
   * @return The schedule ID of the time trigger that triggered the current run
   */
  public ScheduleId getScheduleId() {
    return scheduleId;
  }

  /**
   * @return cron expression of the time trigger
   */
  public String getCronExpression() {
    return cronExpression;
  }

  /**
   * @return properties of the time trigger
   */
  public Map<String, String> getProperties() {
    return properties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TimeTriggeringInfo)) {
      return false;
    }
    TimeTriggeringInfo that = (TimeTriggeringInfo) o;
    return getType() == that.getType()
      && Objects.equals(getScheduleId(), that.getScheduleId())
      && Objects.equals(getCronExpression(), that.getCronExpression())
      && Objects.equals(getProperties(), that.getProperties());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getType(), getScheduleId(), getCronExpression(), getProperties());
  }
}
