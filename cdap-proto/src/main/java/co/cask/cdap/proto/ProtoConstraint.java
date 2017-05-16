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

import java.util.Objects;
import java.util.TimeZone;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Represents a schedule's run constraints in REST requests/repsonses.
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

    private static final Pattern SIMPLE_TIME_PATTERN = Pattern.compile("^[0-9][0-9]:[0-9][0-9]$");

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
      validateSimpleTime("start", startTime);
      validateSimpleTime("end ", endTime);
      ProtoConstraint.validateNotNull(timeZone, "time zone");
      TimeZone.getTimeZone(timeZone);
    }

    private void validateSimpleTime(String name, String time) {
      if (time == null) {
        throw new IllegalArgumentException(name + " time must not be null");
      }
      if (!SIMPLE_TIME_PATTERN.matcher(time).matches()) {
        throw new IllegalArgumentException(
          String.format("%s time must be of the form 'hh:mm' but is '%s'", name, time));
      }
      ProtoConstraint.validateInRange(Integer.valueOf(time.substring(0, 2)), name + " hour", 0, 24);
      ProtoConstraint.validateInRange(Integer.valueOf(time.substring(3, 5)), name + " minute", 0, 60);
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
