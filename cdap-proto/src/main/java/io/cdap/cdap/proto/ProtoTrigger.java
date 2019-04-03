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

import co.cask.cdap.api.ProgramStatus;
import co.cask.cdap.api.schedule.Trigger;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.ProgramId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Represents a trigger in a REST request/response.
 */
public abstract class ProtoTrigger implements Trigger {

  private final Type type;

  private ProtoTrigger(Type type) {
    this.type = type;
  }

  @Override
  public Type getType() {
    return type;
  }

  public abstract void validate();

  /**
   * Represents a time trigger in REST requests/responses.
   */
  public static class TimeTrigger extends ProtoTrigger {

    protected final String cronExpression;

    public TimeTrigger(String cronExpression) {
      super(Type.TIME);
      this.cronExpression = cronExpression;
      validate();
    }

    public String getCronExpression() {
      return cronExpression;
    }

    @Override
    public boolean equals(Object o) {
      return this == o ||
        o != null
          && getClass().equals(o.getClass())
          && Objects.equals(getCronExpression(), ((TimeTrigger) o).getCronExpression());
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(getCronExpression());
    }

    @Override
    public String toString() {
      return "TimeTrigger(" + getCronExpression() + "\")";
    }

    @Override
    public void validate() {
      validateNotNull(getCronExpression(), "cron expression");
    }
  }

  /**
   * Represents a partition trigger in REST requests/responses.
   */
  public static class PartitionTrigger extends ProtoTrigger {

    protected final DatasetId dataset;
    protected final int numPartitions;

    public PartitionTrigger(DatasetId dataset, int numPartitions) {
      super(Type.PARTITION);
      this.dataset = dataset;
      this.numPartitions = numPartitions;
      validate();
    }

    public DatasetId getDataset() {
      return dataset;
    }

    public int getNumPartitions() {
      return numPartitions;
    }

    @Override
    public void validate() {
      ProtoTrigger.validateNotNull(getDataset(), "dataset");
      ProtoTrigger.validateNotNull(getDataset().getNamespace(), "dataset namespace");
      ProtoTrigger.validateNotNull(getDataset().getDataset(), "dataset name");
      ProtoTrigger.validateInRange(getNumPartitions(), "number of partitions", 1, null);
    }

    @Override
    public boolean equals(Object o) {
      return this == o ||
        o != null &&
          getClass().equals(o.getClass()) &&
          Objects.equals(getDataset(), ((PartitionTrigger) o).getDataset()) &&
          Objects.equals(getNumPartitions(), ((PartitionTrigger) o).getNumPartitions());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getDataset(), getNumPartitions());
    }

    @Override
    public String toString() {
      return String.format("PartitionTrigger(%s, %d partitions)", getDataset(), getNumPartitions());
    }
  }

  /**
   * Abstract base class for composite trigger in REST requests/responses.
   *
   * @param <T> type of triggers contained in the composite trigger
   */
  public abstract static class AbstractCompositeTrigger<T extends Trigger> extends ProtoTrigger {
    private final List<T> triggers;

    public AbstractCompositeTrigger(Type type, List<T> triggers) {
      super(type);
      this.triggers = Collections.unmodifiableList(new ArrayList<>(triggers));
      validate();
    }

    public List<T> getTriggers() {
      return triggers;
    }

    @Override
    public void validate() {
      if (!getType().equals(Type.AND) && !getType().equals(Type.OR)) {
        throw new IllegalArgumentException("Trigger type " + getType().name() + " is not a composite trigger.");
      }
      List<T> triggers = getTriggers();
      if (triggers.isEmpty()) {
        throw new IllegalArgumentException(String.format("Triggers passed in to construct a trigger " +
                                                           "of type %s cannot be empty.", getType().name()));
      }
      for (T trigger : triggers) {
        if (trigger == null) {
          throw new IllegalArgumentException(String.format("Triggers passed in to construct a trigger " +
                                                             "of type %s cannot contain null.", getType().name()));
        }
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      AbstractCompositeTrigger that = (AbstractCompositeTrigger) o;
      return triggers.equals(that.getTriggers());
    }

    @Override
    public int hashCode() {
      return triggers.hashCode();
    }

    @Override
    public String toString() {
      return getType() + "Trigger{" +
        "triggers=" + triggers +
        '}';
    }
  }

  /**
   * Shorthand helper method to create an instance of {@link AndTrigger}
   */
  public static AndTrigger and(ProtoTrigger... triggers) {
    return new AndTrigger(Arrays.asList(triggers));
  }

  /**
   * Shorthand helper method to create an instance of {@link OrTrigger}
   */
  public static OrTrigger or(ProtoTrigger... triggers) {
    return new OrTrigger(Arrays.asList(triggers));
  }

  /**
   * Represents an AND trigger in REST requests/responses.
   */
  public static class AndTrigger extends AbstractCompositeTrigger<ProtoTrigger> {
    public AndTrigger(List<ProtoTrigger> triggers) {
      super(Type.AND, triggers);
    }
  }

  /**
   * Represents an OR trigger in REST requests/responses.
   */
  public static class OrTrigger extends AbstractCompositeTrigger<ProtoTrigger> {
    public OrTrigger(List<ProtoTrigger> triggers) {
      super(Type.OR, triggers);
    }
  }

  /**
   * Represents a program status trigger for REST requests/responses
   */
  public static class ProgramStatusTrigger extends ProtoTrigger {
    protected final ProgramId programId;
    protected final Set<ProgramStatus> programStatuses;

    public ProgramStatusTrigger(ProgramId programId, Set<ProgramStatus> programStatuses) {
      super(Type.PROGRAM_STATUS);

      this.programId = programId;
      this.programStatuses = programStatuses;
      validate();
    }

    public ProgramId getProgramId() {
      return programId;
    }

    public Set<ProgramStatus> getProgramStatuses() {
      return programStatuses;
    }

    @Override
    public void validate() {
      if (getProgramStatuses().contains(ProgramStatus.INITIALIZING) ||
          getProgramStatuses().contains(ProgramStatus.RUNNING)) {
        throw new IllegalArgumentException(String.format(
                "Cannot allow triggering program %s with statuses %s: %s statuses are supported",
                programId.getProgram(), getProgramStatuses(), ProgramStatus.TERMINAL_STATES));
      }

      ProtoTrigger.validateNotNull(getProgramId(), "program id");
      ProtoTrigger.validateNotNull(getProgramStatuses(), "program statuses");
    }

    @Override
    public int hashCode() {
      return Objects.hash(getProgramId(), getProgramStatuses());
    }

    @Override
    public boolean equals(Object o) {
      return this == o ||
        o != null &&
          getClass().equals(o.getClass()) &&
          Objects.equals(getProgramStatuses(), ((ProgramStatusTrigger) o).getProgramStatuses()) &&
          Objects.equals(getProgramId(), ((ProgramStatusTrigger) o).getProgramId());
    }

    @Override
    public String toString() {
      return String.format("ProgramStatusTrigger(%s, %s)", getProgramId().getProgram(),
                                                           getProgramStatuses().toString());
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

