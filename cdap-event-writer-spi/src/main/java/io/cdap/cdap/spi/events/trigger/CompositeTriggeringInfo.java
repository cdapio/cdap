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

import java.util.List;
import java.util.Objects;

/**
 * Describes the triggering info for a composite trigger
 */
public class CompositeTriggeringInfo implements TriggeringInfo {
  private final Type type;
  private final ScheduleId scheduleId;
  private final List<TriggeringInfo> triggerInfoList;
  private final TriggeringPropertyMapping propertyMapping;

  public CompositeTriggeringInfo(Type type, ScheduleId scheduleId, List<TriggeringInfo> triggerInfoList,
                                 TriggeringPropertyMapping propertyMapping) {
    this.type = type;
    this.scheduleId = scheduleId;
    this.triggerInfoList = triggerInfoList;
    this.propertyMapping = propertyMapping;
  }

  /**
   * @return The type of the trigger
   */
  @Override
  public Type getType() {
    return type;
  }

  /**
   * @return The schedule ID of the AND/OR trigger that triggered the current run
   */
  public ScheduleId getScheduleId() {
    return scheduleId;
  }

  /**
   * @return The list triggering infos of that triggered the composite trigger
   */
  public List<TriggeringInfo> getTriggeringInfoList() {
    return triggerInfoList;
  }

  /**
   * @return The mapping between triggering pipeline properties to the triggered pipeline arguments
   */
  public TriggeringPropertyMapping getPropertyMapping() {
    return propertyMapping;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CompositeTriggeringInfo)) {
      return false;
    }
    CompositeTriggeringInfo that = (CompositeTriggeringInfo) o;
    return getType() == that.getType()
      && Objects.equals(getScheduleId(), that.getScheduleId())
      && Objects.equals(getTriggeringInfoList(), that.getTriggeringInfoList())
      && Objects.equals(getPropertyMapping(), that.getPropertyMapping());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getType(), getScheduleId(), triggerInfoList, getPropertyMapping());
  }
}
