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

package io.cdap.cdap.etl.spark.function;

import java.io.Serializable;
import javax.annotation.Nullable;

/**
 * DatasetAggregationAccumulator is used to accumulate group values into aggregation value.
 * At given point in time it contains either source group value or (if accumulation started already)
 * an aggregate value.
 * @param <GROUP_VALUE> type of original value to group
 * @param <AGG_VALUE> type of accumulated values after reduce stage
 */
public class DatasetAggregationAccumulator<GROUP_VALUE, AGG_VALUE> implements Serializable {
  @Nullable
  private final GROUP_VALUE groupValue;
  @Nullable
  private final AGG_VALUE aggValue;


  private DatasetAggregationAccumulator(@Nullable GROUP_VALUE groupValue, @Nullable AGG_VALUE aggValue) {
    this.groupValue = groupValue;
    this.aggValue = aggValue;
  }

  public static <GROUP_VALUE, AGG_VALUE> DatasetAggregationAccumulator<GROUP_VALUE, AGG_VALUE> fromGroupValue(
    GROUP_VALUE groupValue) {
    return new DatasetAggregationAccumulator<>(groupValue, null);
  }

  public static <GROUP_VALUE, AGG_VALUE> DatasetAggregationAccumulator<GROUP_VALUE, AGG_VALUE> fromAggValue(
    AGG_VALUE aggValue) {
    return new DatasetAggregationAccumulator<>(null, aggValue);
  }

  @Nullable
  public GROUP_VALUE getGroupValue() {
    return groupValue;
  }

  @Nullable
  public AGG_VALUE getAggValue() {
    return aggValue;
  }

  public boolean hasAggValue() {
    return aggValue != null;
  }
}
