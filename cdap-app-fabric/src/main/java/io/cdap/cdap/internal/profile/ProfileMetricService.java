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

package io.cdap.cdap.internal.profile;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractScheduledService;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.apache.twill.common.Threads;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A service which will emit metrics periodically about a profile, by default it will emit per 60 seconds
 */
public class ProfileMetricService extends AbstractScheduledService {
  private static final long DEFAULT_INTERVAL_MINUTES = 1L;

  private final MetricsContext metricsContext;
  private final long intervalMinutes;
  private final int numNodes;
  private ScheduledExecutorService executor;
  private long startUpTime;

  public ProfileMetricService(MetricsCollectionService metricsCollectionService, ProgramRunId programRunId,
                              ProfileId profileId, int numNodes) {
    this(metricsCollectionService, programRunId, profileId, numNodes, DEFAULT_INTERVAL_MINUTES);
  }

  @VisibleForTesting
  ProfileMetricService(MetricsCollectionService metricsCollectionService, ProgramRunId programRunId,
                       ProfileId profileId, int numNodes, long intervalMinutes) {
    this.metricsContext = getMetricsContextForProfile(metricsCollectionService, programRunId, profileId);
    this.numNodes = numNodes;
    this.intervalMinutes = intervalMinutes;
  }

  @Override
  protected void startUp() throws Exception {
    setStartUpTime(TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()));
  }

  @Override
  protected void shutDown() throws Exception {
    long duration = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()) - startUpTime;
    // if the duration is less than the interval time, we just emit a metric
    if (duration < intervalMinutes * 60) {
      emitMetric();
      // else if the remainder seconds is greater or equal to half of the interval seconds, we emit a metric
    } else if (duration % 60 >= intervalMinutes * 60 / 2) {
      emitMetric();
    }
    if (executor != null) {
      executor.shutdownNow();
    }
  }

  @Override
  protected void runOneIteration() {
    emitMetric();
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(intervalMinutes, intervalMinutes, TimeUnit.MINUTES);
  }

  @Override
  protected final ScheduledExecutorService executor() {
    if (executor != null) {
      return executor;
    }
    executor = Executors.newSingleThreadScheduledExecutor(Threads.createDaemonThreadFactory("profile-metrics"));
    return executor;
  }

  @VisibleForTesting
  void emitMetric() {
    metricsContext.increment(Constants.Metrics.Program.PROGRAM_NODE_MINUTES, intervalMinutes * numNodes);
  }

  @VisibleForTesting
  void setStartUpTime(long startUpTimeSecs) {
    this.startUpTime = startUpTimeSecs;
  }

  /**
   * Get the metrics context for the program, the tags are constructed with the program run id and
   * the profile id
   */
  private MetricsContext getMetricsContextForProfile(MetricsCollectionService metricsCollectionService,
                                                     ProgramRunId programRunId, ProfileId profileId) {
    Map<String, String> tags = ImmutableMap.<String, String>builder()
      .put(Constants.Metrics.Tag.PROFILE_SCOPE, profileId.getScope().name())
      .put(Constants.Metrics.Tag.PROFILE, profileId.getProfile())
      .put(Constants.Metrics.Tag.NAMESPACE, programRunId.getNamespace())
      .put(Constants.Metrics.Tag.PROGRAM_TYPE, programRunId.getType().getPrettyName())
      .put(Constants.Metrics.Tag.APP, programRunId.getApplication())
      .put(Constants.Metrics.Tag.PROGRAM, programRunId.getProgram())
      .put(Constants.Metrics.Tag.RUN_ID, programRunId.getRun())
      .build();

    return metricsCollectionService.getContext(tags);
  }
}
