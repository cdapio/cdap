/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.spi.events;

import io.cdap.cdap.api.schedule.TriggeringScheduleInfo;
import javax.annotation.Nullable;

/**
 * Represents how a run has been triggered and trigger details
 */
public class StartMetadata {

  private final StartType startType;
  @Nullable
  private final TriggeringScheduleInfo triggeringScheduleInfo;

  public StartMetadata(StartType startType,
      @Nullable TriggeringScheduleInfo triggeringScheduleInfo) {
    this.startType = startType;
    this.triggeringScheduleInfo = triggeringScheduleInfo;
  }

  /**
   * @return The type of {@link StartMetadata}
   */
  public StartType getStartType() {
    return startType;
  }

  /**
   * @return The details of the trigger. returns null when the startType is MANUAL
   */
  @Nullable
  public TriggeringScheduleInfo getTriggeringScheduleInfo() {
    return triggeringScheduleInfo;
  }
}
