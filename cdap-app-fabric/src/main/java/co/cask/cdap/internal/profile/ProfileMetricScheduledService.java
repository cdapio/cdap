/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.internal.profile;

import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.id.ProfileId;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractScheduledService;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Profile metrics schedule service which will emit metrics about profile, by default it will emit per 60 seconds
 */
public class ProfileMetricScheduledService extends AbstractScheduledService {
  private static final int DEFAULT_INTERVAL_MIN = 1;

  private final MetricsContext metricsContext;
  private final long intervalMins;
  private final int numNodes;
  private final ScheduledExecutorService executor;

  public ProfileMetricScheduledService(MetricsCollectionService metricsCollectionService, ProgramRunId programRunId,
                                       ProfileId profileId, int numNodes, ScheduledExecutorService executor) {
    this(metricsCollectionService, programRunId, profileId, numNodes, DEFAULT_INTERVAL_MIN, executor);
  }

  public ProfileMetricScheduledService(MetricsCollectionService metricsCollectionService, ProgramRunId programRunId,
                                       ProfileId profileId, int numNodes, long intervalMins,
                                       ScheduledExecutorService executor) {
    this.metricsContext = getMetricsContextForProfile(metricsCollectionService, programRunId, profileId);
    this.numNodes = numNodes;
    this.intervalMins = intervalMins;
    this.executor = executor;
  }

  @Override
  protected void runOneIteration() throws Exception {
    emitMetric();
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(intervalMins, intervalMins, TimeUnit.MINUTES);
  }

  @Override
  protected final ScheduledExecutorService executor() {
    return executor;
  }

  @VisibleForTesting
  void emitMetric() {
    metricsContext.increment(Constants.Metrics.Program.PROGRAM_NODE_MINUTES, intervalMins * numNodes);
  }

  /**
   * Get the metrics context for the program, the tags are constructed with the program run id and
   * the profile id
   */
  private MetricsContext getMetricsContextForProfile(MetricsCollectionService metricsCollectionService,
                                                     ProgramRunId programRunId, ProfileId profileId) {
    Map<String, String> tags = ImmutableMap.<String, String>builder()
      .put(Constants.Metrics.Tag.PROFILE_SCOPE, profileId.getScope().name())
      .put(Constants.Metrics.Tag.PROFILE, profileId.getScopedName())
      .put(Constants.Metrics.Tag.NAMESPACE, programRunId.getNamespace())
      .put(Constants.Metrics.Tag.PROGRAM_TYPE, programRunId.getType().getPrettyName())
      .put(Constants.Metrics.Tag.APP, programRunId.getApplication())
      .put(Constants.Metrics.Tag.PROGRAM, programRunId.getProgram())
      .put(Constants.Metrics.Tag.RUN_ID, programRunId.getRun())
      .build();

    return metricsCollectionService.getContext(tags);
  }
}
