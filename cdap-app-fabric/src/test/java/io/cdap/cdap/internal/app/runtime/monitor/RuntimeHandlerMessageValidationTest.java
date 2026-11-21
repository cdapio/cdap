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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.LoggingEvent;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.dataset.lib.PartitionKey;
import io.cdap.cdap.api.messaging.Message;
import io.cdap.cdap.api.messaging.MessagingContext;
import io.cdap.cdap.api.messaging.TopicAlreadyExistsException;
import io.cdap.cdap.api.metrics.MetricType;
import io.cdap.cdap.api.metrics.MetricValue;
import io.cdap.cdap.api.metrics.MetricValues;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.app.guice.RuntimeServerModule;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.encryption.guice.UserCredentialAeadEncryptionModule;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.guice.LocalLocationModule;
import io.cdap.cdap.common.guice.NoOpAuditLogModule;
import io.cdap.cdap.common.guice.RemoteAuthenticatorModules;
import io.cdap.cdap.common.io.BinaryEncoder;
import io.cdap.cdap.common.io.DatumWriter;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.service.Services;
import io.cdap.cdap.data2.metadata.writer.MetadataMessage;
import io.cdap.cdap.internal.app.runtime.workflow.BasicWorkflowToken;
import io.cdap.cdap.internal.io.ASMDatumWriterFactory;
import io.cdap.cdap.internal.io.ASMFieldAccessorFactory;
import io.cdap.cdap.internal.io.ReflectionSchemaGenerator;
import io.cdap.cdap.logging.appender.LogMessage;
import io.cdap.cdap.logging.context.LoggingContextHelper;
import io.cdap.cdap.logging.serialize.LoggingEventSerializer;
import io.cdap.cdap.messaging.DefaultTopicMetadata;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.cdap.messaging.guice.MessagingServerRuntimeModule;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.audit.AuditMessage;
import io.cdap.cdap.proto.audit.AuditType;
import io.cdap.cdap.proto.audit.payload.access.AccessPayload;
import io.cdap.cdap.proto.audit.payload.access.AccessType;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.Marker;

/**
 * End-to-end HTTP/Avro tests for {@link RuntimeHandler} message validation.
 */
public class RuntimeHandlerMessageValidationTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final Gson GSON = new Gson();
  private static final String CUSTOM_TOPIC_KEY = "custom.test.topic";
  private static final String CUSTOM_TOPIC = "customtopic";
  private static final String MISSING_METRICS_TOPIC_KEY = "missing.metrics.test.topic";

  @Test
  public void testEnforcingAndReportOnlyAcrossAllTopics() throws Exception {
    runSuite(true);
    runSuite(false);
  }

  private void runSuite(boolean enforcing) throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.setBoolean(Constants.RuntimeMonitor.RUNTIME_TOKEN_VALIDATION_ENABLED, enforcing);
    cConf.set(CUSTOM_TOPIC_KEY, CUSTOM_TOPIC);
    String missingMetricsTopic = cConf.get(Constants.Metrics.TOPIC_PREFIX) + "99";
    cConf.set(MISSING_METRICS_TOPIC_KEY, missingMetricsTopic);
    cConf.set(Constants.RuntimeMonitor.TOPICS_CONFIGS,
        Constants.Audit.TOPIC + ","
            + Constants.Dataset.DATA_EVENT_TOPIC + ","
            + Constants.Metadata.MESSAGING_TOPIC + ","
            + Constants.Metrics.TOPIC_PREFIX + ":1,"
            + Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC + ","
            + Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC + ":1,"
            + Constants.Logging.TMS_TOPIC_PREFIX + ":1,"
            + CUSTOM_TOPIC_KEY + ","
            + MISSING_METRICS_TOPIC_KEY);

    Map<String, Long> counters = new ConcurrentHashMap<>();
    MetricsContext metricsContext = Mockito.mock(MetricsContext.class);
    Mockito.doAnswer(invocation -> {
      String name = (String) invocation.getArguments()[0];
      long delta = (Long) invocation.getArguments()[1];
      counters.merge(name, delta, Long::sum);
      return null;
    }).when(metricsContext).increment(Mockito.anyString(), Mockito.anyLong());
    MetricsCollectionService metricsService = Mockito.mock(MetricsCollectionService.class);
    Mockito.when(metricsService.getContext(Mockito.anyMap())).thenReturn(metricsContext);

    Logger reporterLogger = Mockito.mock(Logger.class);
    Mockito.when(reporterLogger.isWarnEnabled()).thenReturn(true);
    Mockito.when(reporterLogger.isWarnEnabled(Mockito.any())).thenReturn(true);
    RuntimeValidationReporter reporter = new RuntimeValidationReporter(
        cConf, metricsService, reporterLogger, () -> 1_000_000L);

    ProgramRunId wfRun = new NamespaceId("ns1").app("app1", "1.0").workflow("wf1")
        .run(RunIds.generate());
    AtomicBoolean useLegacyToken = new AtomicBoolean(false);
    AtomicBoolean failLogProcessor = new AtomicBoolean(false);
    List<byte[]> receivedLogs = new CopyOnWriteArrayList<>();

    Injector injector = Guice.createInjector(
        new ConfigModule(cConf),
        RemoteAuthenticatorModules.getNoOpModule(),
        new NoOpAuditLogModule(),
        new UserCredentialAeadEncryptionModule(),
        new InMemoryDiscoveryModule(),
        new LocalLocationModule(),
        new MessagingServerRuntimeModule().getInMemoryModules(),
        new AuthenticationContextModules().getNoOpModule(),
        new AuthorizationEnforcementModule().getNoOpModules(),
        new RuntimeServerModule() {
          @Override
          protected void bindRequestValidator() {
            bind(RuntimeRequestValidator.class).toInstance((programRunId, request) -> {
              ProgramRunInfo info = new ProgramRunInfo(Long.MAX_VALUE);
              if (useLegacyToken.get()) {
                return ValidatedRuntimeRequest.legacy(info);
              }
              return ValidatedRuntimeRequest.runtimeToken(info, wfRun, null);
            });
          }

          @Override
          protected void bindLogProcessor() {
            bind(RemoteExecutionLogProcessor.class).toInstance(payloads -> {
              if (failLogProcessor.get()) {
                throw new IllegalStateException("Injected log processor failure");
              }
              Assert.assertTrue("Log processor must not be called on empty block",
                  payloads.hasNext());
              payloads.forEachRemaining(receivedLogs::add);
            });
          }
        },
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class);
            bind(RuntimeValidationReporter.class).toInstance(reporter);
          }
        });

    MessagingService messagingService = injector.getInstance(MessagingService.class);
    if (messagingService instanceof Service) {
      Services.startAndWait((Service) messagingService);
    }
    RuntimeServer runtimeServer = injector.getInstance(RuntimeServer.class);
    try {
      String logsTopic = cConf.get(Constants.Logging.TMS_TOPIC_PREFIX) + "0";
      String metricsTopic = cConf.get(Constants.Metrics.TOPIC_PREFIX) + "0";
      String metadataTopic = cConf.get(Constants.Metadata.MESSAGING_TOPIC);
      String auditTopic = cConf.get(Constants.Audit.TOPIC);
      String dataEventTopic = cConf.get(Constants.Dataset.DATA_EVENT_TOPIC);
      String bareStatusTopic = cConf.get(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC);
      String numberedStatusTopic = bareStatusTopic + "0";

      for (String topic : Arrays.asList(metricsTopic, metadataTopic, auditTopic, dataEventTopic,
          bareStatusTopic, numberedStatusTopic, CUSTOM_TOPIC)) {
        try {
          messagingService.createTopic(new DefaultTopicMetadata(NamespaceId.SYSTEM.topic(topic)));
        } catch (TopicAlreadyExistsException ignored) {
          // System topics are pre-created by MessagingService on startup.
        }
      }

      Services.startAndWait(runtimeServer);
      RuntimeClient client = injector.getInstance(RuntimeClient.class);

      // 1. Logs: mixed batch (1 valid, 2 invalid) -> single log line per request
      ProgramRunId otherNsRun = new NamespaceId("ns2").app("app1", "1.0").workflow("wf1")
          .run(RunIds.generate());
      byte[] validLog = encodeLog(
          LoggingContextHelper.getLoggingContextWithRunId(wfRun, null));
      byte[] badLog = encodeLog(
          LoggingContextHelper.getLoggingContextWithRunId(otherNsRun, null));
      byte[] undecodable = "not-valid".getBytes(StandardCharsets.UTF_8);
      client.sendMessages(wfRun, NamespaceId.SYSTEM.topic(logsTopic),
          asMessages(validLog, badLog, undecodable).iterator());
      assertPayloadListEquals(
          enforcing ? Collections.singletonList(validLog)
              : Arrays.asList(validLog, badLog, undecodable),
          receivedLogs);
      Assert.assertEquals(Long.valueOf(2L),
          counters.get("runtime.token.validation.failure.message_logs"));
      Mockito.verify(reporterLogger, Mockito.times(1)).debug(Mockito.anyString(),
          Mockito.eq("message_logs"), Mockito.eq(wfRun), Mockito.eq(logsTopic),
          Mockito.eq(2L), Mockito.eq(enforcing));
      Mockito.verify(reporterLogger, Mockito.times(1)).warn(Mockito.isNull(Marker.class),
          Mockito.anyString(), Mockito.eq("message_logs"), Mockito.eq(wfRun),
          Mockito.eq(logsTopic), Mockito.eq(2L), Mockito.eq(enforcing));

      // All-dropped logs block: log processor must not be called and request succeeds
      receivedLogs.clear();
      client.sendMessages(wfRun, NamespaceId.SYSTEM.topic(logsTopic),
          asMessages(badLog).iterator());
      assertPayloadListEquals(
          enforcing ? Collections.emptyList() : Collections.singletonList(badLog),
          receivedLogs);
      Assert.assertEquals(Long.valueOf(3L),
          counters.get("runtime.token.validation.failure.message_logs"));

      // 2. Metrics: block containing ONLY embedded-service metrics -> not relayed when enforcing,
      //    never counted, never logged, DirectMessagePublisher not called on empty block
      MessagingContext messagingContext = new MultiThreadMessagingContext(messagingService);
      byte[] embeddedMetric = encodeMetric(ImmutableMap.of(
          Constants.Metrics.Tag.NAMESPACE, NamespaceId.SYSTEM.getNamespace(),
          Constants.Metrics.Tag.COMPONENT, Constants.Service.MESSAGING_SERVICE));
      client.sendMessages(wfRun, NamespaceId.SYSTEM.topic(metricsTopic),
          asMessages(embeddedMetric, embeddedMetric).iterator());
      assertPayloadListEquals(
          enforcing ? Collections.emptyList() : Arrays.asList(embeddedMetric, embeddedMetric),
          fetchPayloads(messagingContext, metricsTopic));
      Assert.assertFalse(counters.containsKey("runtime.token.validation.failure.message_metrics"));
      Mockito.verify(reporterLogger, Mockito.never()).debug(Mockito.anyString(),
          Mockito.eq("message_metrics"), Mockito.any(), Mockito.any(), Mockito.anyLong(),
          Mockito.anyBoolean());
      Mockito.verify(reporterLogger, Mockito.never()).warn(Mockito.isNull(Marker.class),
          Mockito.anyString(), Mockito.eq("message_metrics"), Mockito.any(), Mockito.any(),
          Mockito.anyLong(), Mockito.anyBoolean());

      // Mixed metrics batch (1 valid, 1 invalid, 1 embedded)
      byte[] validMetric = encodeMetric(ImmutableMap.of(
          Constants.Metrics.Tag.NAMESPACE, "ns1",
          Constants.Metrics.Tag.APP, "app1",
          Constants.Metrics.Tag.RUN_ID, wfRun.getRun()));
      byte[] badMetric = encodeMetric(ImmutableMap.of(
          Constants.Metrics.Tag.NAMESPACE, "ns2",
          Constants.Metrics.Tag.APP, "app1",
          Constants.Metrics.Tag.RUN_ID, wfRun.getRun()));
      client.sendMessages(wfRun, NamespaceId.SYSTEM.topic(metricsTopic),
          asMessages(validMetric, badMetric, embeddedMetric).iterator());
      assertPayloadListEquals(
          enforcing ? Collections.singletonList(validMetric)
              : Arrays.asList(embeddedMetric, embeddedMetric, validMetric, badMetric,
                  embeddedMetric),
          fetchPayloads(messagingContext, metricsTopic));
      Assert.assertEquals(Long.valueOf(1L),
          counters.get("runtime.token.validation.failure.message_metrics"));

      // 3. Metadata: multi-block request with multiple drops -> ONE log line per request
      BasicWorkflowToken wfToken = new BasicWorkflowToken(1);
      wfToken.setCurrentNode("n1");
      wfToken.put("k", "v");
      byte[] validMetadata = GSON.toJson(new MetadataMessage(
          MetadataMessage.Type.WORKFLOW_TOKEN, wfRun, GSON.toJsonTree(wfToken)))
          .getBytes(StandardCharsets.UTF_8);
      // Make two large invalid payloads (> CHUNK_SIZE) so RuntimeClient emits multiple Avro blocks
      String padding = repeat('a', RuntimeClient.CHUNK_SIZE + 64);
      byte[] badMetadata1 = GSON.toJson(new MetadataMessage(
          MetadataMessage.Type.PROFILE_ASSIGNMENT, wfRun, GSON.toJsonTree(padding)))
          .getBytes(StandardCharsets.UTF_8);
      byte[] badMetadata2 = GSON.toJson(new MetadataMessage(
          MetadataMessage.Type.LINEAGE, otherNsRun, GSON.toJsonTree(padding)))
          .getBytes(StandardCharsets.UTF_8);
      client.sendMessages(wfRun, NamespaceId.SYSTEM.topic(metadataTopic),
          asMessages(badMetadata1, validMetadata, badMetadata2).iterator());
      assertPayloadListEquals(
          enforcing ? Collections.singletonList(validMetadata)
              : Arrays.asList(badMetadata1, validMetadata, badMetadata2),
          fetchPayloads(messagingContext, metadataTopic));
      Assert.assertEquals(Long.valueOf(2L),
          counters.get("runtime.token.validation.failure.message_metadata"));
      Mockito.verify(reporterLogger, Mockito.times(1)).debug(Mockito.anyString(),
          Mockito.eq("message_metadata"), Mockito.eq(wfRun), Mockito.eq(metadataTopic),
          Mockito.eq(2L), Mockito.eq(enforcing));
      Mockito.verify(reporterLogger, Mockito.times(1)).warn(Mockito.isNull(Marker.class),
          Mockito.anyString(), Mockito.eq("message_metadata"), Mockito.eq(wfRun),
          Mockito.eq(metadataTopic), Mockito.eq(2L), Mockito.eq(enforcing));

      // All-invalid metadata block -> DirectMessagePublisher not called, request succeeds
      client.sendMessages(wfRun, NamespaceId.SYSTEM.topic(metadataTopic),
          asMessages(undecodable).iterator());
      assertPayloadListEquals(
          enforcing ? Collections.singletonList(validMetadata)
              : Arrays.asList(badMetadata1, validMetadata, badMetadata2, undecodable),
          fetchPayloads(messagingContext, metadataTopic));
      Assert.assertEquals(Long.valueOf(3L),
          counters.get("runtime.token.validation.failure.message_metadata"));

      // 4. Audit: mixed batch
      byte[] validAudit = GSON.toJson(new AuditMessage(System.currentTimeMillis(),
          new NamespaceId("ns1").dataset("ds1"), "u", AuditType.ACCESS,
          new AccessPayload(AccessType.READ, wfRun)))
          .getBytes(StandardCharsets.UTF_8);
      byte[] badAudit = GSON.toJson(new AuditMessage(System.currentTimeMillis(),
          new NamespaceId("ns2").dataset("ds1"), "u", AuditType.ACCESS,
          new AccessPayload(AccessType.READ, wfRun)))
          .getBytes(StandardCharsets.UTF_8);
      client.sendMessages(wfRun, NamespaceId.SYSTEM.topic(auditTopic),
          asMessages(validAudit, badAudit).iterator());
      assertPayloadListEquals(
          enforcing ? Collections.singletonList(validAudit) : Arrays.asList(validAudit, badAudit),
          fetchPayloads(messagingContext, auditTopic));
      Assert.assertEquals(Long.valueOf(1L),
          counters.get("runtime.token.validation.failure.message_audit"));

      // 5. Data events: mixed batch
      PartitionKey key = PartitionKey.builder().addStringField("k", "v").build();
      byte[] validDataEvent = GSON.toJson(Notification.forPartitions(
          new NamespaceId("ns1").dataset("ds1"), Collections.singletonList(key)))
          .getBytes(StandardCharsets.UTF_8);
      byte[] badDataEvent = GSON.toJson(Notification.forPartitions(
          new NamespaceId("ns2").dataset("ds1"), Collections.singletonList(key)))
          .getBytes(StandardCharsets.UTF_8);
      client.sendMessages(wfRun, NamespaceId.SYSTEM.topic(dataEventTopic),
          asMessages(validDataEvent, badDataEvent).iterator());
      assertPayloadListEquals(
          enforcing ? Collections.singletonList(validDataEvent)
              : Arrays.asList(validDataEvent, badDataEvent),
          fetchPayloads(messagingContext, dataEventTopic));
      Assert.assertEquals(Long.valueOf(1L),
          counters.get("runtime.token.validation.failure.message_data_event"));

      // 6. Program status topics (bare and numbered) pass through untouched and uncounted
      final Map<String, Long> countersBeforeStatus = new HashMap<>(counters);
      byte[] statusPayload = "{\"arbitrary\":\"status\"}".getBytes(StandardCharsets.UTF_8);
      client.sendMessages(wfRun, NamespaceId.SYSTEM.topic(bareStatusTopic),
          asMessages(statusPayload).iterator());
      client.sendMessages(wfRun, NamespaceId.SYSTEM.topic(numberedStatusTopic),
          asMessages(statusPayload).iterator());
      assertPayloadListEquals(Collections.singletonList(statusPayload),
          fetchPayloads(messagingContext, bareStatusTopic));
      assertPayloadListEquals(Collections.singletonList(statusPayload),
          fetchPayloads(messagingContext, numberedStatusTopic));
      Assert.assertEquals(countersBeforeStatus, counters);

      // 6b. Validation and reporting use the token's run id (wfRun), not the URL's run id
      ProgramRunId innerSparkSnapshotUrl = new NamespaceId("ns1")
          .app("app1", "-SNAPSHOT").spark("innerSpark").run(RunIds.generate());
      receivedLogs.clear();
      client.sendMessages(innerSparkSnapshotUrl, NamespaceId.SYSTEM.topic(logsTopic),
          asMessages(validLog, badLog, badLog, badLog).iterator());
      assertPayloadListEquals(
          enforcing ? Collections.singletonList(validLog)
              : Arrays.asList(validLog, badLog, badLog, badLog),
          receivedLogs);
      Assert.assertEquals(Long.valueOf(6L),
          counters.get("runtime.token.validation.failure.message_logs"));
      Mockito.verify(reporterLogger, Mockito.times(1)).debug(Mockito.anyString(),
          Mockito.eq("message_logs"), Mockito.eq(wfRun), Mockito.eq(logsTopic),
          Mockito.eq(3L), Mockito.eq(enforcing));

      // 7. Other allowed topic: dropped when enforcing, forwarded in report-only, counted
      byte[] customPayload = "custom-msg".getBytes(StandardCharsets.UTF_8);
      client.sendMessages(wfRun, NamespaceId.SYSTEM.topic(CUSTOM_TOPIC),
          asMessages(customPayload, customPayload).iterator());
      assertPayloadListEquals(
          enforcing ? Collections.emptyList() : Arrays.asList(customPayload, customPayload),
          fetchPayloads(messagingContext, CUSTOM_TOPIC));
      Assert.assertEquals(Long.valueOf(2L),
          counters.get("runtime.token.validation.failure.message_topic"));

      // 7b & 7c. Error paths: chunk() BadRequestException (no double count) and handleError() only
      verifyErrorPathDropFlushing(client, wfRun, missingMetricsTopic, logsTopic,
          badMetric, validMetric, badLog, validLog, failLogProcessor, counters,
          reporterLogger, enforcing);

      // 8. Legacy token request bypasses message validation even when enforcing
      useLegacyToken.set(true);
      final Map<String, Long> countersBeforeLegacy = new HashMap<>(counters);
      client.sendMessages(wfRun, NamespaceId.SYSTEM.topic(CUSTOM_TOPIC),
          asMessages(customPayload).iterator());
      assertPayloadListEquals(
          enforcing ? Collections.singletonList(customPayload)
              : Arrays.asList(customPayload, customPayload, customPayload),
          fetchPayloads(messagingContext, CUSTOM_TOPIC));
      Assert.assertEquals(countersBeforeLegacy, counters);
    } finally {
      Services.stopAndWait(runtimeServer);
      if (messagingService instanceof Service) {
        Services.stopAndWait((Service) messagingService);
      }
    }
  }

  private static byte[] encodeLog(LoggingContext loggingContext) {
    LoggerContext ctx = new LoggerContext();
    LoggingEvent event = new LoggingEvent("io.cdap.test", ctx.getLogger("test"),
        Level.INFO, "msg", null, null);
    return new LoggingEventSerializer().toBytes(new LogMessage(event, loggingContext));
  }

  private static byte[] encodeMetric(Map<String, String> tags) throws Exception {
    Schema schema = new ReflectionSchemaGenerator().generate(MetricValues.class);
    DatumWriter<MetricValues> writer = new ASMDatumWriterFactory(new ASMFieldAccessorFactory())
        .create(TypeToken.of(MetricValues.class), schema);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writer.encode(new MetricValues(tags, System.currentTimeMillis() / 1000L,
        Collections.singletonList(new MetricValue("m", MetricType.COUNTER, 1L))),
        new BinaryEncoder(out));
    return out.toByteArray();
  }

  private static List<Message> asMessages(byte[]... payloads) {
    List<Message> messages = new ArrayList<>(payloads.length);
    for (int i = 0; i < payloads.length; i++) {
      final String id = "id-" + i;
      final byte[] bytes = payloads[i];
      messages.add(new Message() {
        @Override
        public String getId() {
          return id;
        }

        @Override
        public byte[] getPayload() {
          return bytes;
        }
      });
    }
    return messages;
  }

  private static List<byte[]> fetchPayloads(MessagingContext context, String topic)
      throws Exception {
    List<byte[]> result = new ArrayList<>();
    try (CloseableIterator<Message> iter = context.getMessageFetcher().fetch(
        NamespaceId.SYSTEM.getNamespace(), topic, 100, 0L)) {
      while (iter.hasNext()) {
        result.add(iter.next().getPayload());
      }
    }
    return result;
  }

  private static void assertPayloadListEquals(List<byte[]> expected, List<byte[]> actual) {
    Assert.assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      Assert.assertArrayEquals("Mismatch at payload index " + i, expected.get(i), actual.get(i));
    }
  }

  private static void verifyErrorPathDropFlushing(RuntimeClient client, ProgramRunId wfRun,
      String missingMetricsTopic, String logsTopic, byte[] badMetric, byte[] validMetric,
      byte[] badLog, byte[] validLog, AtomicBoolean failLogProcessor, Map<String, Long> counters,
      Logger reporterLogger, boolean enforcing) throws Exception {
    // chunk() error path: publish fails with BadRequestException on missing TMS topic;
    // drop counter increments once (chunk() flushes, handleError() does not double-count)
    final long metricsDropsBefore = counters.getOrDefault(
        "runtime.token.validation.failure.message_metrics", 0L);
    try {
      client.sendMessages(wfRun, NamespaceId.SYSTEM.topic(missingMetricsTopic),
          asMessages(badMetric, validMetric).iterator());
      Assert.fail("Expected BadRequestException when publishing to a missing TMS topic");
    } catch (BadRequestException expected) {
      // expected HTTP 400
    }
    Assert.assertEquals(Long.valueOf(metricsDropsBefore + 1L),
        counters.get("runtime.token.validation.failure.message_metrics"));
    Mockito.verify(reporterLogger, Mockito.times(1)).debug(Mockito.anyString(),
        Mockito.eq("message_metrics"), Mockito.eq(wfRun), Mockito.eq(missingMetricsTopic),
        Mockito.eq(1L), Mockito.eq(enforcing));

    // handleError() path only: unchecked exception in log processor flushes drop count once
    final long logDropsBefore = counters.getOrDefault(
        "runtime.token.validation.failure.message_logs", 0L);
    Mockito.clearInvocations(reporterLogger);
    failLogProcessor.set(true);
    try {
      client.sendMessages(wfRun, NamespaceId.SYSTEM.topic(logsTopic),
          asMessages(badLog, validLog).iterator());
      Assert.fail("Expected error response when log processor throws unchecked exception");
    } catch (IOException expected) {
      // expected HTTP 500 error response
    } finally {
      failLogProcessor.set(false);
    }
    Assert.assertEquals(Long.valueOf(logDropsBefore + 1L),
        counters.get("runtime.token.validation.failure.message_logs"));
    Mockito.verify(reporterLogger, Mockito.times(1)).debug(Mockito.anyString(),
        Mockito.eq("message_logs"), Mockito.eq(wfRun), Mockito.eq(logsTopic),
        Mockito.eq(1L), Mockito.eq(enforcing));
  }

  private static String repeat(char c, int count) {
    char[] chars = new char[count];
    Arrays.fill(chars, c);
    return new String(chars);
  }
}
