/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.app.metrics;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Metrics collector for MapReduce job.
 * todo: extract TaskType enum in its own class
 */
public final class MapReduceMetrics {
  public static final String METRIC_INPUT_RECORDS = "process.entries.in";
  public static final String METRIC_OUTPUT_RECORDS = "process.entries.out";
  public static final String METRIC_BYTES = "process.bytes";
  public static final String METRIC_COMPLETION = "process.completion";
  public static final String METRIC_TASK_INPUT_RECORDS = "process.entries.task.in";
  public static final String METRIC_TASK_OUTPUT_RECORDS = "process.entries.task.out";
  public static final String METRIC_TASK_BYTES = "process.task.bytes";

  public static final String METRIC_TASK_COMPLETION = "process.completion.task";
  public static final String METRIC_USED_CONTAINERS = "resources.used.containers";
  public static final String METRIC_USED_MEMORY = "resources.used.memory";

  /**
   * Type of map reduce task.
   */
  public enum TaskType {
    Mapper("m"),
    Reducer("r");

    private static final Map<org.apache.hadoop.mapreduce.TaskType, TaskType> LOOKUP_BY_MAP =
      ImmutableMap.of(org.apache.hadoop.mapreduce.TaskType.MAP, Mapper,
                      org.apache.hadoop.mapreduce.TaskType.REDUCE, Reducer);

    private final String id;

    TaskType(String id) {
      this.id = id;
    }

    public String getId() {
      return id;
    }

    public static boolean hasType(org.apache.hadoop.mapreduce.TaskType hadoopTaskType) {
      return LOOKUP_BY_MAP.containsKey(hadoopTaskType);
    }

    /**
     * @return the corresponding {@link TaskType} for the given {@link org.apache.hadoop.mapreduce.TaskType}, or throws
     * IllegalArgumentException if there is no corresponding TaskType.
     */
    public static TaskType from(org.apache.hadoop.mapreduce.TaskType hadoopTaskType) {
      Preconditions.checkArgument(LOOKUP_BY_MAP.containsKey(hadoopTaskType),
                                  "No TaskType for value: %s.", hadoopTaskType);
      return LOOKUP_BY_MAP.get(hadoopTaskType);
    }
  }

}
