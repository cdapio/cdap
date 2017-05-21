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

import co.cask.cdap.internal.schedule.constraint.Constraint;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Objects;
import java.util.TimeZone;
import javax.annotation.Nullable;

/**
 * Represents a schedule's run constraints in REST requests/responses.
 */
public abstract class ProtoConstraint implements Constraint {

  private final Type type;

  /**
   * The type of a run constraint.
   */
  public enum Type {
    CONCURRENCY,
    DELAY,
    TIME_RANGE,
    LAST_RUN
  }

  private ProtoConstraint(Type type) {
    this.type = type;
  }

  public Type getType() {
    return type;
  }

  public abstract void validate();

  /**
   * Represents a concurrency constraint in REST requests/responses.
   */
  public static class ConcurrenyConstraint extends ProtoConstraint {

    protected final int maxConcurrency;

    public ConcurrenyConstraint(int maxConcurrency) {
      super(Type.CONCURRENCY);
      this.maxConcurrency = maxConcurrency;
      validate();
    }

    public int getMaxConcurrency() {
      return maxConcurrency;
    }

    @Override
    public void validate() {
      validateInRange(maxConcurrency, "maxConcurrency", 1, null);
    }

    @Override
    public boolean equals(Object o) {
      return this == o || o != null
        && getClass() == o.getClass()
        && getMaxConcurrency() == ((ConcurrenyConstraint) o).getMaxConcurrency();
    }

    @Override
    public int hashCode() {
      return getMaxConcurrency();
    }

    @Override
    public String toString() {
      return String.format("Concurrency(max %d)", getMaxConcurrency());
    }
  }

  /**
   * Represents a delay constraint in REST requests/responses.
   */
  public static class DelayConstraint extends ProtoConstraint {

    protected final long millisAfterTrigger;

    public DelayConstraint(long millisAfterTrigger) {
      super(Type.DELAY);
      this.millisAfterTrigger = millisAfterTrigger;
      validate();
    }

    public long getMillisAfterTrigger() {
      return millisAfterTrigger;
    }

    @Override
    public void validate() {
      validateInRange(millisAfterTrigger, "millisAfterTrigger", 1L, null);
    }

    @Override
    public boolean equals(Object o) {
      return this == o || o != null
        && getClass() == o.getClass()
        && getMillisAfterTrigger() == ((DelayConstraint) o).getMillisAfterTrigger();
    }

    @Override
    public int hashCode() {
      return (int) (getMillisAfterTrigger() ^ (getMillisAfterTrigger() >>> 32));
    }

    @Override
    public String toString() {
      return String.format("Delay(%d ms)", getMillisAfterTrigger());
    }
  }

  /**
   * Represents a last run constraint in REST requests/responses.
   */
  public static class LastRunConstraint extends ProtoConstraint {

    protected final long millisSinceLastRun;

    public LastRunConstraint(long millisSinceLastRun) {
      super(Type.LAST_RUN);
      this.millisSinceLastRun = millisSinceLastRun;
      validate();
    }

    public long getMillisSinceLastRun() {
      return millisSinceLastRun;
    }

    @Override
    public void validate() {
      validateInRange(millisSinceLastRun, "millisSinceLastRun", 1L, null);
    }

    @Override
    public boolean equals(Object o) {
      return this == o || o != null
        && getClass() == o.getClass()
        && getMillisSinceLastRun() == ((LastRunConstraint) o).getMillisSinceLastRun();
    }

    @Override
    public int hashCode() {
      return (int) (getMillisSinceLastRun() ^ (getMillisSinceLastRun() >>> 32));
    }

    @Override
    public String toString() {
      return String.format("LastRun(%d ms)", getMillisSinceLastRun());
    }
  }

  /**
   * Represents a time range constraint in REST requests/responses.
   */
  public static class TimeRangeConstraint extends ProtoConstraint {

    protected final String timeZone;
    protected final String startTime;
    protected final String endTime;

    public TimeRangeConstraint(String startTime,
                               String endTime,
                               TimeZone timeZone) {
      super(Type.TIME_RANGE);
      this.startTime = startTime;
      this.endTime = endTime;
      this.timeZone = timeZone.getID();
      validate();
    }

    public String getStartTime() {
      return startTime;
    }

    public String getEndTime() {
      return endTime;
    }

    public String getTimeZone() {
      return timeZone;
    }

    @Override
    public void validate() {
      doValidate();
    }

    /**
     * Performs the validation. Can be called by subclasses to validate and initialize.
     *
     * @return Calendar, start and end date as a ValidationResult.
     */
    protected ValidationResult doValidate() {
      ProtoConstraint.validateNotNull(timeZone, "time zone");
      TimeZone tz = TimeZone.getTimeZone(timeZone);
      Calendar calendar = Calendar.getInstance(tz);
      DateFormat formatter = new SimpleDateFormat("HH:mm");
      formatter.setTimeZone(tz);
      Date startDate, endDate;
      try {
        startDate = formatter.parse(startTime);
      } catch (ParseException e) {
        throw new IllegalArgumentException(String.format("Failed to parse start time '%s'", startTime), e);
      }
      try {
        endDate = formatter.parse(endTime);
      } catch (ParseException e) {
        throw new IllegalArgumentException(String.format("Failed to parse end time '%s'", endTime), e);
      }
      if (startDate.compareTo(endDate) >= 0) {
        throw new IllegalArgumentException("The start time (%s) must be before the end time (%s).");
      }
      return new ValidationResult(calendar, startDate, endDate);
    }

    @Override
    public boolean equals(Object o) {
      return this == o || o != null
        && getClass() == o.getClass()
        && getStartTime().equals(((TimeRangeConstraint) o).getStartTime())
        && getEndTime().equals(((TimeRangeConstraint) o).getEndTime())
        && Objects.equals(getTimeZone(), ((TimeRangeConstraint) o).getTimeZone());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getStartTime(), getEndTime(), getTimeZone());
    }

    @Override
    public String toString() {
      return String.format("TimeRange(%s-%s in %s)", getStartTime(), getEndTime(), getTimeZone());
    }

    /**
     * Helper class to capture the intermediate values of validation, to avoid code duplication.
     */
    protected static class ValidationResult {
      private final Calendar calendar;
      private final Date startDate;
      private final Date endDate;

      ValidationResult(Calendar calendar, Date startDate, Date endDate) {
        this.calendar = calendar;
        this.startDate = startDate;
        this.endDate = endDate;
      }

      public Date getStartDate() {
        return startDate;
      }

      public Date getEndDate() {
        return endDate;
      }

      public Calendar getCalendar() {
        return calendar;
      }
    }
  }

  private static void validateNotNull(@Nullable Object o, String name) {
    if (o == null) {
      throw new IllegalArgumentException(name + " must not be null");
    }
  }

  private static <V extends Comparable<V>>
  void validateInRange(@Nullable V value, String name, @Nullable V minValue, @Nullable V maxValue) {
    if (value == null) {
      throw new IllegalArgumentException(name + " must be specified");
    }
    if (minValue != null && value.compareTo(minValue) < 0) {
      throw new IllegalArgumentException(name + " must be greater than or equal to" + minValue + " but is " + value);
    }
    if (maxValue != null && value.compareTo(maxValue) > 0) {
      throw new IllegalArgumentException(name + " must be less than or equal to " + maxValue + " but is " + value);
    }
  }
}
