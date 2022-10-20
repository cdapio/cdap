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

package io.cdap.cdap.proto;

import io.cdap.cdap.api.schedule.Trigger;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.id.ScheduleId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Class contains the triggering information for a run
 */
public abstract class TriggeringInfo implements Trigger {
  private final Type type;
  private final ScheduleId scheduleId;
  /**
   * Runtime Arguments propagated from upstream program or properties of time schedule
   */
  @Nullable
  private final Map<String, String> runtimeArguments;

  protected TriggeringInfo(Type type, ScheduleId scheduleId, @Nullable Map<String, String> runtimeArguments) {
    this.type = type;
    this.scheduleId = scheduleId;
    this.runtimeArguments = Collections.unmodifiableMap(runtimeArguments);
  }

  @Override
  public Type getType() {
    return type;
  }

  public ScheduleId getScheduleId() {
    return scheduleId;
  }

  @Nullable
  public Map<String, String> getRuntimeArguments() {
    return runtimeArguments;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TriggeringInfo)) {
      return false;
    }
    TriggeringInfo that = (TriggeringInfo) o;
    return Objects.equals(getType(), that.getType())
      && Objects.equals(getScheduleId(), that.getScheduleId())
      && Objects.equals(getRuntimeArguments(), that.getRuntimeArguments());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getType(), getScheduleId(), getRuntimeArguments());
  }

  /**
   * Represents Triggering info of a time trigger
   */
  public static class TimeTriggeringInfo extends TriggeringInfo {
    private final String cronExpression;

    public TimeTriggeringInfo(ScheduleId scheduleId, Map<String, String> runtimeArguments, String cronExpression) {
      super(Type.TIME, scheduleId, runtimeArguments);
      this.cronExpression = cronExpression;
    }

    public String getCronExpression() {
      return cronExpression;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof TimeTriggeringInfo)) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      TimeTriggeringInfo that = (TimeTriggeringInfo) o;
      return Objects.equals(getCronExpression(), that.getCronExpression());
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), getCronExpression());
    }
  }

  /**
   * Represents Triggering info of a ProgramStatus Trigger
   */
  public static class ProgramStatusTriggeringInfo extends TriggeringInfo {
    private final ProgramRunId programRunId;

    public ProgramStatusTriggeringInfo(ScheduleId scheduleId, Map<String, String> runtimeArguments,
                                          ProgramRunId programRunId) {
      super(Type.PROGRAM_STATUS, scheduleId, runtimeArguments);
      this.programRunId = programRunId;
    }

    public ProgramRunId getProgramRunId() {
      return programRunId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ProgramStatusTriggeringInfo)) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      ProgramStatusTriggeringInfo that = (ProgramStatusTriggeringInfo) o;
      return Objects.equals(getProgramRunId(), that.getProgramRunId());
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), getProgramRunId());
    }
  }

  /**
   * Represents Triggering info of a Partition Trigger
   */
  public static class PartitionTriggeringInfo extends TriggeringInfo {
    private final String datasetNamespace;
    private final String datasetName;
    private final int expectedNumPartitions;
    private final int actualNumPartitions;

    public PartitionTriggeringInfo(ScheduleId scheduleId, @Nullable Map<String, String> runtimeArguments,
                                      String datasetName, String datasetNamespace, int expectedNumPartitions,
                                      int actualNumPartitions) {
      super(Type.PARTITION, scheduleId, runtimeArguments);
      this.datasetName = datasetName;
      this.datasetNamespace = datasetNamespace;
      this.expectedNumPartitions = expectedNumPartitions;
      this.actualNumPartitions = actualNumPartitions;
    }

    public String getDatasetNamespace() {
      return datasetNamespace;
    }

    public String getDatasetName() {
      return datasetName;
    }

    public int getExpectedNumPartitions() {
      return expectedNumPartitions;
    }

    public int getActualNumPartitions() {
      return actualNumPartitions;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof PartitionTriggeringInfo)) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      PartitionTriggeringInfo that = (PartitionTriggeringInfo) o;
      return getExpectedNumPartitions() == that.getExpectedNumPartitions()
        && getActualNumPartitions() == that.getActualNumPartitions()
        && Objects.equals(getDatasetNamespace(), that.getDatasetNamespace())
        && Objects.equals(getDatasetName(), that.getDatasetName());
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), getDatasetNamespace(), getDatasetName(),
                          getExpectedNumPartitions(), getActualNumPartitions());
    }
  }

  /**
   * Abstract base class of a composite TrigerringInfo
   * @param <T>
   */
  public abstract static class AbstractCompositeTriggeringInfo<T extends TriggeringInfo> extends TriggeringInfo {
    private final List<T> triggeringInfos;
    @Nullable
    private final TriggeringPropertyMapping triggeringPropertyMapping;

    protected AbstractCompositeTriggeringInfo(Type type, List<T> triggeringInfos, ScheduleId scheduleId,
                                              @Nullable Map<String, String> runtimeArguments,
                                              @Nullable TriggeringPropertyMapping triggeringPropertyMapping) {
      super(type, scheduleId, runtimeArguments);
      this.triggeringInfos = Collections.unmodifiableList(new ArrayList<>(triggeringInfos));
      this.triggeringPropertyMapping = triggeringPropertyMapping;
    }

    public List<T> getTriggeringInfos() {
      return triggeringInfos;
    }

    public TriggeringPropertyMapping getTriggeringPropertyMapping() {
      return triggeringPropertyMapping;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof AbstractCompositeTriggeringInfo)) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      AbstractCompositeTriggeringInfo<?> that = (AbstractCompositeTriggeringInfo<?>) o;
      return Objects.equals(getTriggeringInfos(), that.getTriggeringInfos())
        && Objects.equals(getTriggeringPropertyMapping(), that.getTriggeringPropertyMapping());
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), getTriggeringInfos(), getTriggeringPropertyMapping());
    }
  }

  /**
   * Represents Triggering info for a composite OR trigger
   */
  public static class OrTriggeringInfo extends AbstractCompositeTriggeringInfo<TriggeringInfo> {

    public OrTriggeringInfo(List<TriggeringInfo> triggeringInfos, ScheduleId scheduleId,
                            @Nullable Map<String, String> runtimeArguments,
                            @Nullable TriggeringPropertyMapping triggeringPropertyMapping) {
      super(Type.OR, triggeringInfos, scheduleId, runtimeArguments, triggeringPropertyMapping);
    }
  }

  /**
   * Represents Triggering info for a composite AND trigger
   */
  public static class AndTriggeringInfo extends AbstractCompositeTriggeringInfo<TriggeringInfo> {

    public AndTriggeringInfo(List<TriggeringInfo> triggeringInfos, ScheduleId scheduleId,
                             @Nullable Map<String, String> runtimeArguments,
                             @Nullable TriggeringPropertyMapping triggeringPropertyMapping) {
      super(Type.AND, triggeringInfos, scheduleId, runtimeArguments, triggeringPropertyMapping);
    }
  }
}
