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

import co.cask.cdap.internal.app.runtime.schedule.ProgramSchedule;
import co.cask.cdap.proto.ProtoConstraint;
import com.google.common.base.Preconditions;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * A Constraint that defines a time range in which the schedule is allowed to execute.
 */
public class TimeRangeConstraint extends ProtoConstraint.TimeRangeConstraint implements CheckableConstraint {

  // only is satisfied within the range [startTime, endTime)
  // this are transient so they don't get serialized to Json
  private transient int startHour;
  private transient int startMinute;
  private transient int endHour;
  private transient int endMinute;

  private transient Calendar calendar;

  public TimeRangeConstraint(String startTime, String endTime, TimeZone timeZone) {
    super(startTime, endTime, timeZone);
  }

  @Override
  public void validate() {
    initialize();
  }

  private void initialize() {
    if (calendar == null) {
      TimeZone tz = TimeZone.getTimeZone(timeZone);
      calendar = Calendar.getInstance(tz);
      DateFormat formatter = new SimpleDateFormat("HH:mm");
      formatter.setTimeZone(tz);
      Date startDate, endDate;
      try {
        startDate = formatter.parse(startTime);
        calendar.setTime(startDate);
        startHour = calendar.get(Calendar.HOUR_OF_DAY);
        startMinute = calendar.get(Calendar.MINUTE);
      } catch (ParseException e) {
        throw new IllegalArgumentException(String.format("Failed to parse start time '%s'", startTime), e);
      }
      try {
        endDate = formatter.parse(endTime);
        calendar.setTime(endDate);
        endHour = calendar.get(Calendar.HOUR_OF_DAY);
        endMinute = calendar.get(Calendar.MINUTE);
      } catch (ParseException e) {
        throw new IllegalArgumentException(String.format("Failed to parse end time '%s'", endTime), e);
      }
      Preconditions.checkArgument(startDate.compareTo(endDate) < 0, "The start time must be before the end time.");
    }
  }

  @Override
  public ConstraintResult check(ProgramSchedule schedule, ConstraintContext context) {
    initialize();
    calendar.setTimeInMillis(context.getCheckTime());
    int hourOfDay = calendar.get(Calendar.HOUR_OF_DAY);
    int minute = calendar.get(Calendar.MINUTE);

    boolean pastOrEqualStartRange = hourOfDay > startHour || (hourOfDay == startHour && minute >= startMinute);
    boolean pastEndRange = hourOfDay > endHour || (hourOfDay == endHour && minute >= endMinute);

    boolean satisfied = pastOrEqualStartRange && !pastEndRange;
    if (satisfied) {
      return ConstraintResult.SATISFIED;
    }

    if (pastEndRange) {
      // we've past the end time range for today
      calendar.add(Calendar.DAY_OF_YEAR, 1);
    }
    calendar.set(Calendar.HOUR_OF_DAY, startHour);
    calendar.set(Calendar.MINUTE, startMinute);
    calendar.set(Calendar.SECOND, 0);
    calendar.set(Calendar.MILLISECOND, 0);
    return new ConstraintResult(ConstraintResult.SatisfiedState.NOT_SATISFIED,
                                calendar.getTimeInMillis() - context.getCheckTime());
  }
}
