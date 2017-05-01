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

package co.cask.cdap.internal.app.runtime.schedule.constraint;

import co.cask.cdap.internal.schedule.constraint.Constraint;

import java.util.TimeZone;

/**
 * A Constraint that defines a time range in which the schedule is allowed to execute.
 */
public class TimeRangeConstraint extends Constraint {

  // only is satisfied within the range [startHour, endHour)
  // TODO: Allow minute granularity
  private final int startHour;
  private final int endHour;
  private final TimeZone timeZone;

  public TimeRangeConstraint(int startHour, int endHour, TimeZone timeZone) {
    this.startHour = startHour;
    this.endHour = endHour;
    this.timeZone = timeZone;
  }
}
