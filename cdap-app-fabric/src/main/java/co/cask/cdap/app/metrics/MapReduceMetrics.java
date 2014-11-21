/*
 * Copyright © 2014 Cask Data, Inc.
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

import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.common.metrics.MetricsScope;
import co.cask.cdap.internal.app.program.TypeId;
import co.cask.cdap.proto.ProgramType;

/**
 * Metrics collector for MapReduce job.
 */
public final class MapReduceMetrics extends AbstractProgramMetrics {

  /**
   * Type of map reduce task.
   */
  public enum TaskType {
    Mapper("m"),
    Reducer("r");

    private final String id;

    private TaskType(String id) {
      this.id = id;
    }

    public String getId() {
      return id;
    }
  }

  public MapReduceMetrics(MetricsCollectionService collectionService, String applicationId,
                          String mapReduceId, TaskType type, String runId) {
    super(collectionService.getCollector(
      MetricsScope.USER,
      String.format("%s.%s.%s.%s", applicationId, TypeId.getMetricContextId(ProgramType.MAPREDUCE), mapReduceId,
                    type.getId()), runId));
  }
}
