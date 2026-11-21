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
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.TimeProvider;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.Marker;

/**
 * Unit tests for {@link RuntimeValidationReporter}.
 */
public class RuntimeValidationReporterTest {

  @Test
  public void testCountsPerReasonInSystemRuntimeContext() {
    Map<String, String> capturedTags = new HashMap<>();
    Map<String, Long> counters = new HashMap<>();
    MetricsContext metricsContext = Mockito.mock(MetricsContext.class);
    Mockito.doAnswer(invocation -> {
      String metricName = (String) invocation.getArguments()[0];
      long delta = (Long) invocation.getArguments()[1];
      counters.merge(metricName, delta, Long::sum);
      return null;
    }).when(metricsContext).increment(Mockito.anyString(), Mockito.anyLong());

    MetricsCollectionService metricsService = Mockito.mock(MetricsCollectionService.class);
    Mockito.when(metricsService.getContext(Mockito.anyMapOf(String.class, String.class)))
        .thenAnswer(invocation -> {
          @SuppressWarnings("unchecked")
          Map<String, String> tags = (Map<String, String>) invocation.getArguments()[0];
          capturedTags.putAll(tags);
          return metricsContext;
        });

    CConfiguration cConf = CConfiguration.create();
    Assert.assertFalse(
        cConf.getBoolean(Constants.RuntimeMonitor.RUNTIME_TOKEN_VALIDATION_ENABLED, false));
    Logger logger = Mockito.mock(Logger.class);
    AtomicLong now = new AtomicLong(100_000L);
    RuntimeValidationReporter reporter = new RuntimeValidationReporter(
        cConf, metricsService, logger, now::get);

    Assert.assertEquals(ImmutableMap.of(
        Constants.Metrics.Tag.NAMESPACE, NamespaceId.SYSTEM.getNamespace(),
        Constants.Metrics.Tag.COMPONENT, Constants.Service.RUNTIME), capturedTags);

    ProgramRunId runId = NamespaceId.DEFAULT.app("app", "1.0").workflow("wf").run(RunIds.generate());
    for (RuntimeValidationFailure failure : RuntimeValidationFailure.values()) {
      Assert.assertFalse(reporter.reportFailure(failure, runId, Constants.Service.RUNTIME,
          "GET", "/test"));
      Assert.assertEquals(Long.valueOf(1L), counters.get(failure.getMetricName()));
    }
    Assert.assertEquals("runtime.token.validation.failure.secret",
        RuntimeValidationFailure.SECRET.getMetricName());
    Assert.assertEquals("runtime.token.validation.failure.connection",
        RuntimeValidationFailure.CONNECTION.getMetricName());
    Assert.assertEquals("runtime.token.validation.failure.oauth",
        RuntimeValidationFailure.OAUTH.getMetricName());

    // Verify enforcement flag propagates
    cConf.setBoolean(Constants.RuntimeMonitor.RUNTIME_TOKEN_VALIDATION_ENABLED, true);
    RuntimeValidationReporter enforcingReporter = new RuntimeValidationReporter(
        cConf, metricsService, logger, now::get);
    Assert.assertTrue(enforcingReporter.reportFailure(RuntimeValidationFailure.URL_MISMATCH,
        runId, Constants.Service.RUNTIME, "POST", "/p"));
    Assert.assertEquals(Long.valueOf(2L),
        counters.get(RuntimeValidationFailure.URL_MISMATCH.getMetricName()));
  }

  @Test
  public void testWarnRateLimitedPerReasonWithInjectedTime() {
    MetricsContext metricsContext = Mockito.mock(MetricsContext.class);
    MetricsCollectionService metricsService = Mockito.mock(MetricsCollectionService.class);
    Mockito.when(metricsService.getContext(Mockito.anyMapOf(String.class, String.class)))
        .thenReturn(metricsContext);

    Logger logger = Mockito.mock(Logger.class);
    Mockito.when(logger.isWarnEnabled()).thenReturn(true);
    Mockito.when(logger.isWarnEnabled(Mockito.any())).thenReturn(true);
    Mockito.when(logger.isDebugEnabled()).thenReturn(true);
    Mockito.when(logger.isDebugEnabled(Mockito.any())).thenReturn(true);
    AtomicLong now = new AtomicLong(1_000_000L);

    RuntimeValidationReporter reporter = new RuntimeValidationReporter(
        CConfiguration.create(), metricsService, logger, now::get);
    ProgramRunId runId = NamespaceId.DEFAULT.app("app", "1.0").spark("s").run(RunIds.generate());

    // First LEGACY_TOKEN emits WARN and DEBUG
    reporter.reportFailure(RuntimeValidationFailure.LEGACY_TOKEN, runId,
        Constants.Service.RUNTIME, "GET", "/a");
    // Second LEGACY_TOKEN within 10s emits DEBUG only, not WARN
    now.addAndGet(5_000L);
    reporter.reportFailure(RuntimeValidationFailure.LEGACY_TOKEN, runId,
        Constants.Service.RUNTIME, "GET", "/b");
    // Different reason (URL_MISMATCH) has its own independent rate limiter, so it emits WARN
    reporter.reportFailure(RuntimeValidationFailure.URL_MISMATCH, runId,
        Constants.Service.APP_FABRIC_HTTP, "POST", "/c");
    // After 10s since first LEGACY_TOKEN, LEGACY_TOKEN emits WARN again
    now.addAndGet(RuntimeValidationReporter.WARN_RATE_LIMIT_MILLIS);
    reporter.reportFailure(RuntimeValidationFailure.LEGACY_TOKEN, runId,
        Constants.Service.RUNTIME, "GET", "/d");

    // DEBUG always logs with exact fields (4 calls)
    Mockito.verify(logger).debug(Mockito.anyString(),
        Mockito.eq("legacy_token"), Mockito.eq(runId), Mockito.eq(Constants.Service.RUNTIME),
        Mockito.eq("GET"), Mockito.eq("/a"), Mockito.eq(false));
    Mockito.verify(logger).debug(Mockito.anyString(),
        Mockito.eq("legacy_token"), Mockito.eq(runId), Mockito.eq(Constants.Service.RUNTIME),
        Mockito.eq("GET"), Mockito.eq("/b"), Mockito.eq(false));
    Mockito.verify(logger).debug(Mockito.anyString(),
        Mockito.eq("url_mismatch"), Mockito.eq(runId),
        Mockito.eq(Constants.Service.APP_FABRIC_HTTP), Mockito.eq("POST"), Mockito.eq("/c"),
        Mockito.eq(false));
    Mockito.verify(logger).debug(Mockito.anyString(),
        Mockito.eq("legacy_token"), Mockito.eq(runId), Mockito.eq(Constants.Service.RUNTIME),
        Mockito.eq("GET"), Mockito.eq("/d"), Mockito.eq(false));

    // WARN logs 3 times (/a, /c, /d), skipping rate-limited /b
    Mockito.verify(logger).warn(Mockito.isNull(Marker.class), Mockito.anyString(),
        Mockito.eq("legacy_token"), Mockito.eq(runId), Mockito.eq(Constants.Service.RUNTIME),
        Mockito.eq("GET"), Mockito.eq("/a"), Mockito.eq(false));
    Mockito.verify(logger, Mockito.never()).warn(Mockito.isNull(Marker.class), Mockito.anyString(),
        Mockito.eq("legacy_token"), Mockito.eq(runId), Mockito.eq(Constants.Service.RUNTIME),
        Mockito.eq("GET"), Mockito.eq("/b"), Mockito.eq(false));
    Mockito.verify(logger).warn(Mockito.isNull(Marker.class), Mockito.anyString(),
        Mockito.eq("url_mismatch"), Mockito.eq(runId),
        Mockito.eq(Constants.Service.APP_FABRIC_HTTP), Mockito.eq("POST"), Mockito.eq("/c"),
        Mockito.eq(false));
    Mockito.verify(logger).warn(Mockito.isNull(Marker.class), Mockito.anyString(),
        Mockito.eq("legacy_token"), Mockito.eq(runId), Mockito.eq(Constants.Service.RUNTIME),
        Mockito.eq("GET"), Mockito.eq("/d"), Mockito.eq(false));
  }

  @Test
  public void testReportAdmissionCountsAndLogsDebugWithoutWarn() {
    Map<String, String> capturedTags = new HashMap<>();
    Map<String, Long> counters = new HashMap<>();
    MetricsContext metricsContext = Mockito.mock(MetricsContext.class);
    Mockito.doAnswer(invocation -> {
      String metricName = (String) invocation.getArguments()[0];
      long delta = (Long) invocation.getArguments()[1];
      counters.merge(metricName, delta, Long::sum);
      return null;
    }).when(metricsContext).increment(Mockito.anyString(), Mockito.anyLong());

    MetricsCollectionService metricsService = Mockito.mock(MetricsCollectionService.class);
    Mockito.when(metricsService.getContext(Mockito.anyMap()))
        .thenAnswer(invocation -> {
          @SuppressWarnings("unchecked")
          Map<String, String> tags = (Map<String, String>) invocation.getArguments()[0];
          capturedTags.putAll(tags);
          return metricsContext;
        });

    for (boolean enforcing : new boolean[]{false, true}) {
      counters.clear();
      capturedTags.clear();
      Logger logger = Mockito.mock(Logger.class);
      Mockito.when(logger.isWarnEnabled()).thenReturn(true);
      Mockito.when(logger.isWarnEnabled(Mockito.any())).thenReturn(true);

      CConfiguration cConf = CConfiguration.create();
      cConf.setBoolean(Constants.RuntimeMonitor.RUNTIME_TOKEN_VALIDATION_ENABLED, enforcing);
      RuntimeValidationReporter reporter = new RuntimeValidationReporter(
          cConf, metricsService, logger, TimeProvider.SYSTEM_TIME);

      Assert.assertEquals(ImmutableMap.of(
          Constants.Metrics.Tag.NAMESPACE, NamespaceId.SYSTEM.getNamespace(),
          Constants.Metrics.Tag.COMPONENT, Constants.Service.RUNTIME), capturedTags);

      ProgramRunId runId = NamespaceId.DEFAULT.app("app", "1.0").workflow("wf")
          .run(RunIds.generate());
      reporter.reportAdmission(RuntimeScopeAdmission.DYNAMIC_SECRET, runId, "svc", "GET", "/s");
      reporter.reportAdmission(RuntimeScopeAdmission.WILDCARD_CONNECTION, runId, "svc", "GET",
          "/c");
      reporter.reportAdmission(RuntimeScopeAdmission.WILDCARD_OAUTH, runId, "svc", "GET", "/o");

      Assert.assertEquals(Long.valueOf(1L), counters.get("runtime.token.dynamic.secret.read"));
      Assert.assertEquals(Long.valueOf(1L),
          counters.get("runtime.token.wildcard.connection.read"));
      Assert.assertEquals(Long.valueOf(1L), counters.get("runtime.token.wildcard.oauth.read"));
      Assert.assertEquals(3, counters.size());

      Mockito.verify(logger).debug(Mockito.anyString(),
          Mockito.eq("dynamic_secret"), Mockito.eq(runId), Mockito.eq("svc"),
          Mockito.eq("GET"), Mockito.eq("/s"));
      Mockito.verify(logger).debug(Mockito.anyString(),
          Mockito.eq("wildcard_connection"), Mockito.eq(runId), Mockito.eq("svc"),
          Mockito.eq("GET"), Mockito.eq("/c"));
      Mockito.verify(logger).debug(Mockito.anyString(),
          Mockito.eq("wildcard_oauth"), Mockito.eq(runId), Mockito.eq("svc"),
          Mockito.eq("GET"), Mockito.eq("/o"));
      // Verifies no warn() or warn-level check was invoked at all.
      Mockito.verifyNoMoreInteractions(logger);
    }
  }
}
