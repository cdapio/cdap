/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.monitor;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.logging.LogSamplers;
import io.cdap.cdap.common.logging.Loggers;
import io.cdap.cdap.common.utils.TimeProvider;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Counts and rate-limit logs runtime token validation failures, records non-failure scope
 * admissions, and reports whether enforcement is enabled.
 */
@Singleton
final class RuntimeValidationReporter {

  private static final Logger LOG = LoggerFactory.getLogger(RuntimeValidationReporter.class);
  private static final String FAILURE_LOG_FORMAT =
      "Runtime token validation failure: reason={}, runId={}, service={}, method={}, path={}, "
          + "enforced={}";
  private static final String ADMISSION_LOG_FORMAT =
      "Runtime token scope admission: reason={}, runId={}, service={}, method={}, path={}";
  static final long WARN_RATE_LIMIT_MILLIS = TimeUnit.SECONDS.toMillis(10);

  private final boolean validationEnabled;
  private final MetricsContext metricsContext;
  private final Logger logger;
  private final Map<RuntimeValidationFailure, Logger> warnLoggers;

  @Inject
  RuntimeValidationReporter(CConfiguration cConf, MetricsCollectionService metricsCollectionService) {
    this(cConf, metricsCollectionService, LOG, TimeProvider.SYSTEM_TIME);
  }

  RuntimeValidationReporter(CConfiguration cConf, MetricsCollectionService metricsCollectionService,
      Logger logger, TimeProvider timeProvider) {
    this.validationEnabled = cConf.getBoolean(
        Constants.RuntimeMonitor.RUNTIME_TOKEN_VALIDATION_ENABLED, false);
    this.metricsContext = metricsCollectionService.getContext(ImmutableMap.of(
        Constants.Metrics.Tag.NAMESPACE, NamespaceId.SYSTEM.getNamespace(),
        Constants.Metrics.Tag.COMPONENT, Constants.Service.RUNTIME));
    this.logger = logger;
    EnumMap<RuntimeValidationFailure, Logger> loggers =
        new EnumMap<>(RuntimeValidationFailure.class);
    for (RuntimeValidationFailure failure : RuntimeValidationFailure.values()) {
      loggers.put(failure,
          Loggers.sampling(logger, LogSamplers.limitRate(WARN_RATE_LIMIT_MILLIS, timeProvider)));
    }
    this.warnLoggers = loggers;
  }

  /**
   * Records a request-level validation failure (metric counter, DEBUG log, and rate-limited WARN
   * log) and returns {@code true} if enforcement is enabled and the caller should reject the
   * request.
   */
  boolean reportFailure(RuntimeValidationFailure failure, ProgramRunId runId,
      String service, String method, String path) {
    metricsContext.increment(failure.getMetricName(), 1L);
    logger.debug(FAILURE_LOG_FORMAT,
        failure.getReason(), runId, service, method, path, validationEnabled);
    warnLoggers.get(failure).warn(FAILURE_LOG_FORMAT,
        failure.getReason(), runId, service, method, path, validationEnabled);
    return validationEnabled;
  }

  /**
   * Records a non-failure scope admission (metric counter and DEBUG log only, never WARN).
   */
  void reportAdmission(RuntimeScopeAdmission admission, ProgramRunId runId,
      String service, String method, String path) {
    metricsContext.increment(admission.getMetricName(), 1L);
    logger.debug(ADMISSION_LOG_FORMAT, admission.getReason(), runId, service, method, path);
  }
}
