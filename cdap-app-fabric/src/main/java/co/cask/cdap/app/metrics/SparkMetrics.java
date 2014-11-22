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

import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.common.metrics.MetricsScope;
import co.cask.cdap.internal.app.program.TypeId;
import co.cask.cdap.proto.ProgramType;

/**
 * Metrics collector for {@link Spark} Programs
 */
public final class SparkMetrics extends AbstractProgramMetrics {

  public SparkMetrics(MetricsCollectionService collectionService, String applicationId,
                      String mapReduceId, String runId) {
    super(collectionService.getCollector(
      MetricsScope.USER,
      String.format("%s.%s.%s", applicationId, TypeId.getMetricContextId(ProgramType.SPARK), mapReduceId), runId));
  }
}
