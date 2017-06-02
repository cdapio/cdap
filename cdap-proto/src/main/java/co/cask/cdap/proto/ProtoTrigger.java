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

import co.cask.cdap.internal.schedule.trigger.Trigger;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.StreamId;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Represents a trigger in a REST request/response.
 */
public abstract class ProtoTrigger implements Trigger {

  /**
   * Represents all known trigger types in REST requests/responses.
   */
  public enum Type {
    TIME,
    PARTITION,
    STREAM_SIZE
  }

  private final Type type;

  private ProtoTrigger(Type type) {
    this.type = type;
  }

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
   * Represents a stream size trigger in REST requests/responses.
   */
  public static class StreamSizeTrigger extends ProtoTrigger {

    protected final StreamId streamId;
    protected final int triggerMB;

    public StreamSizeTrigger(StreamId streamId, int triggerMB) {
      super(Type.STREAM_SIZE);
      this.streamId = streamId;
      this.triggerMB = triggerMB;
      validate();
    }

    public StreamId getStreamId() {
      return streamId;
    }

    public int getTriggerMB() {
      return triggerMB;
    }

    @Override
    public void validate() {
      ProtoTrigger.validateNotNull(getStreamId(), "stream");
      ProtoTrigger.validateNotNull(getStreamId().getNamespace(), "stream namespace");
      ProtoTrigger.validateNotNull(getStreamId().getStream(), "stream name");
      ProtoTrigger.validateInRange(getTriggerMB(), "trigger in MB", 1, null);
    }

    @Override
    public boolean equals(Object o) {
      return this == o ||
        o != null &&
          getClass().equals(o.getClass()) &&
          Objects.equals(getStreamId(), ((StreamSizeTrigger) o).getStreamId()) &&
          Objects.equals(getTriggerMB(), ((StreamSizeTrigger) o).getTriggerMB());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getStreamId(), getTriggerMB());
    }

    @Override
    public String toString() {
      return String.format("StreamSizeTrigger(%s, %d MB)", getStreamId(), getTriggerMB());
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

