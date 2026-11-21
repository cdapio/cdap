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
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.LoggingEvent;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import io.cdap.cdap.api.data.schema.UnsupportedTypeException;
import io.cdap.cdap.api.dataset.lib.PartitionKey;
import io.cdap.cdap.api.lineage.field.EndPoint;
import io.cdap.cdap.api.lineage.field.InputField;
import io.cdap.cdap.api.lineage.field.Operation;
import io.cdap.cdap.api.lineage.field.ReadOperation;
import io.cdap.cdap.api.lineage.field.TransformOperation;
import io.cdap.cdap.api.lineage.field.WriteOperation;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.api.metrics.MetricValues;
import io.cdap.cdap.api.workflow.NodeStatus;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.io.DatumWriter;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.logging.NamespaceLoggingContext;
import io.cdap.cdap.common.logging.ServiceLoggingContext;
import io.cdap.cdap.data2.audit.DefaultAuditPublisher;
import io.cdap.cdap.data2.metadata.lineage.AccessType;
import io.cdap.cdap.data2.metadata.lineage.field.FieldLineageInfo;
import io.cdap.cdap.data2.metadata.writer.MessagingLineageWriter;
import io.cdap.cdap.data2.metadata.writer.MessagingMetadataPublisher;
import io.cdap.cdap.data2.metadata.writer.MetadataMessage;
import io.cdap.cdap.data2.metadata.writer.MetadataOperation;
import io.cdap.cdap.data2.registry.MessagingUsageWriter;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.monitor.RuntimeMessageValidator.Decision;
import io.cdap.cdap.internal.app.runtime.monitor.RuntimeMessageValidator.TopicKind;
import io.cdap.cdap.internal.app.runtime.workflow.BasicWorkflowToken;
import io.cdap.cdap.internal.app.runtime.workflow.MessagingWorkflowStateWriter;
import io.cdap.cdap.internal.io.DatumWriterFactory;
import io.cdap.cdap.internal.io.SchemaGenerator;
import io.cdap.cdap.internal.profile.ProfileMetricService;
import io.cdap.cdap.logging.appender.LogMessage;
import io.cdap.cdap.logging.appender.tms.TMSLogAppender;
import io.cdap.cdap.logging.context.ApplicationLoggingContext;
import io.cdap.cdap.logging.context.LoggingContextHelper;
import io.cdap.cdap.logging.context.MapReduceLoggingContext;
import io.cdap.cdap.logging.context.UserServiceLoggingContext;
import io.cdap.cdap.logging.context.WorkflowLoggingContext;
import io.cdap.cdap.logging.context.WorkflowProgramLoggingContext;
import io.cdap.cdap.logging.serialize.LoggingEventSerializer;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.messaging.spi.StoreRequest;
import io.cdap.cdap.metrics.collect.MessagingMetricsCollectionService;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.WorkflowNodeStateDetail;
import io.cdap.cdap.proto.audit.AuditPayload;
import io.cdap.cdap.proto.audit.AuditType;
import io.cdap.cdap.proto.audit.payload.access.AccessPayload;
import io.cdap.cdap.proto.codec.OperationTypeAdapter;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.PluginId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link RuntimeMessageValidator}.
 */
public class RuntimeMessageValidatorTest {

  private static final Gson GSON = new Gson();
  private static final Gson OPERATION_GSON = new GsonBuilder()
      .registerTypeAdapter(Operation.class, new OperationTypeAdapter())
      .create();

  private CConfiguration cConf;
  private RuntimeMessageValidator validator;
  private ProgramRunId workflowRun;
  private ProgramRunId sparkRun;
  private ProgramRunId innerSparkRun;
  private ProgramRunId otherVersionWorkflowRun;
  private ProgramRunId otherAppWorkflowRun;

  private List<byte[]> capturedPayloads;
  private MessagingService recordingMessagingService;
  private Injector publisherInjector;

  @Before
  public void setUp() throws Exception {
    cConf = CConfiguration.create();
    validator = new RuntimeMessageValidator(cConf);
    workflowRun = new NamespaceId("ns1").app("app1", "1.0").workflow("wf1")
        .run(RunIds.generate());
    sparkRun = new NamespaceId("ns1").app("app1", "1.0").spark("sp1")
        .run(RunIds.generate());
    innerSparkRun = new NamespaceId("ns1").app("app1", "1.0").spark("innerSpark")
        .run(RunIds.generate());
    otherVersionWorkflowRun = new NamespaceId("ns1").app("app1", "2.0").workflow("wf1")
        .run(workflowRun.getRun());
    otherAppWorkflowRun = new NamespaceId("ns1").app("otherApp", "1.0").workflow("wf1")
        .run(RunIds.generate());

    capturedPayloads = new CopyOnWriteArrayList<>();
    recordingMessagingService = Mockito.mock(MessagingService.class);
    Mockito.when(recordingMessagingService.publish(Mockito.any(StoreRequest.class)))
        .thenAnswer(invocation -> {
          StoreRequest req = (StoreRequest) invocation.getArguments()[0];
          for (byte[] bytes : req) {
            capturedPayloads.add(bytes);
          }
          return null;
        });

    publisherInjector = createPublisherInjector(false);
  }

  private Injector createPublisherInjector(boolean withMetricsWriter) {
    if (!withMetricsWriter) {
      return Guice.createInjector(
          new ConfigModule(cConf),
          new AbstractModule() {
            @Override
            protected void configure() {
              bind(MessagingService.class).toInstance(recordingMessagingService);
            }
          });
    }
    return Guice.createInjector(
        new ConfigModule(cConf),
        new IOModule(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(MessagingService.class).toInstance(recordingMessagingService);
          }

          @Provides
          DatumWriter<MetricValues> provideMetricWriter(SchemaGenerator schemaGenerator,
              DatumWriterFactory writerFactory) throws UnsupportedTypeException {
            TypeToken<MetricValues> type = TypeToken.of(MetricValues.class);
            return writerFactory.create(type, schemaGenerator.generate(type.getType()));
          }
        });
  }

  private byte[] takeSinglePayload() {
    Assert.assertEquals("Expected exactly one published payload", 1, capturedPayloads.size());
    return capturedPayloads.remove(0);
  }

  private byte[] encodeLogViaAppender(LoggingContext loggingContext) {
    return encodeLogViaAppender(loggingContext, Collections.emptyMap());
  }

  private byte[] encodeLogViaAppender(LoggingContext loggingContext, Map<String, String> extraMdc) {
    capturedPayloads.clear();
    TMSLogAppender appender = publisherInjector.getInstance(TMSLogAppender.class);
    LoggerContext loggerContext = new LoggerContext();
    appender.setContext(loggerContext);
    appender.start();
    try {
      Logger log = loggerContext.getLogger("io.cdap.test");
      LoggingEvent event = new LoggingEvent("io.cdap.test", log, Level.INFO, "hello", null, null);
      if (!extraMdc.isEmpty()) {
        event.setMDCPropertyMap(new HashMap<>(extraMdc));
      }
      appender.doAppend(new LogMessage(event, loggingContext));
    } finally {
      appender.stop();
    }
    return takeSinglePayload();
  }

  private byte[] encodeRawLogMdc(Map<String, String> mdc) {
    LoggerContext loggerContext = new LoggerContext();
    LoggingEvent event = new LoggingEvent("io.cdap.test", loggerContext.getLogger("test"),
        Level.INFO, "raw", null, null);
    event.setMDCPropertyMap(new HashMap<>(mdc));
    return new LoggingEventSerializer().toBytes(event);
  }

  private byte[] encodeMetricViaService(Map<String, String> tags) {
    capturedPayloads.clear();
    // MessagingMetricsCollectionService is @Singleton, so instantiate a fresh service per call.
    MessagingMetricsCollectionService metricsService =
        createPublisherInjector(true).getInstance(MessagingMetricsCollectionService.class);
    metricsService.startAndWait();
    try {
      metricsService.getContext(tags).increment("test.metric", 1L);
    } finally {
      metricsService.stopAndWait();
    }
    return takeSinglePayload();
  }

  private byte[] encodeProfileMetricViaService(ProgramRunId runId, ProfileId profileId) {
    capturedPayloads.clear();
    MessagingMetricsCollectionService metricsService =
        createPublisherInjector(true).getInstance(MessagingMetricsCollectionService.class);
    metricsService.startAndWait();
    ProfileMetricService profileMetricService =
        new ProfileMetricService(metricsService, runId, profileId, 3);
    profileMetricService.startAndWait();
    try {
      profileMetricService.stopAndWait();
    } finally {
      metricsService.stopAndWait();
    }
    return takeSinglePayload();
  }

  @Test
  public void testTopicClassification() {
    Assert.assertEquals(TopicKind.LOGS, validator.classifyTopic("logs0"));
    Assert.assertEquals(TopicKind.LOGS, validator.classifyTopic("logs9"));
    Assert.assertEquals(TopicKind.LOGS, validator.classifyTopic("logs"));
    Assert.assertEquals(TopicKind.METRICS, validator.classifyTopic("metrics0"));
    Assert.assertEquals(TopicKind.METRICS, validator.classifyTopic("metrics12"));
    Assert.assertEquals(TopicKind.METADATA, validator.classifyTopic("metadata"));
    Assert.assertEquals(TopicKind.AUDIT, validator.classifyTopic("audit"));
    Assert.assertEquals(TopicKind.DATA_EVENT, validator.classifyTopic("dataevent"));
    Assert.assertEquals(TopicKind.PROGRAM_STATUS, validator.classifyTopic("programstatusevent"));
    Assert.assertEquals(TopicKind.PROGRAM_STATUS, validator.classifyTopic("programstatusevent0"));
    Assert.assertEquals(TopicKind.PROGRAM_STATUS, validator.classifyTopic("programstatusevent9"));

    Assert.assertEquals(TopicKind.OTHER, validator.classifyTopic("logs_custom"));
    Assert.assertEquals(TopicKind.OTHER, validator.classifyTopic("metrics_custom"));
    Assert.assertEquals(TopicKind.OTHER, validator.classifyTopic("programstatusevent_custom"));
    Assert.assertEquals(TopicKind.OTHER, validator.classifyTopic("custom_topic"));
    Assert.assertEquals(TopicKind.OTHER, validator.classifyTopic(null));
    Assert.assertEquals(TopicKind.OTHER, validator.classifyTopic(""));

    Assert.assertEquals(Decision.ACCEPT,
        validator.validate(TopicKind.PROGRAM_STATUS, "anything".getBytes(StandardCharsets.UTF_8),
            workflowRun));
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.OTHER, "anything".getBytes(StandardCharsets.UTF_8),
            workflowRun));
  }

  @Test
  public void testLogsValidation() {
    // 1. Workflow's own log passes (arbitrary non-identity user MDC keys are preserved)
    LoggingContext wfCtx = LoggingContextHelper.getLoggingContextWithRunId(workflowRun, null);
    Assert.assertEquals(Decision.ACCEPT,
        validator.validate(TopicKind.LOGS,
            encodeLogViaAppender(wfCtx, ImmutableMap.of("userKey", "userVal")), workflowRun));

    // 2. Inner Spark and inner MapReduce of workflow carry workflow program + workflow run id
    Map<String, String> wfSysArgs = ImmutableMap.of(
        ProgramOptionConstants.WORKFLOW_NAME, workflowRun.getProgram(),
        ProgramOptionConstants.WORKFLOW_RUN_ID, workflowRun.getRun());
    LoggingContext innerSparkCtx =
        LoggingContextHelper.getLoggingContextWithRunId(innerSparkRun, wfSysArgs);
    Assert.assertEquals(Decision.ACCEPT,
        validator.validate(TopicKind.LOGS, encodeLogViaAppender(innerSparkCtx), workflowRun));

    ProgramRunId innerMrRun = new NamespaceId("ns1").app("app1", "1.0").mr("innerMr")
        .run(RunIds.generate());
    LoggingContext innerMrCtx =
        LoggingContextHelper.getLoggingContextWithRunId(innerMrRun, wfSysArgs);
    Assert.assertEquals(Decision.ACCEPT,
        validator.validate(TopicKind.LOGS, encodeLogViaAppender(innerMrCtx), workflowRun));

    // 3. Top-level Spark, MapReduce, Worker, and Service contexts pass for their own token run
    LoggingContext topSparkCtx = LoggingContextHelper.getLoggingContextWithRunId(sparkRun, null);
    Assert.assertEquals(Decision.ACCEPT,
        validator.validate(TopicKind.LOGS, encodeLogViaAppender(topSparkCtx), sparkRun));

    ProgramRunId topMrRun = new NamespaceId("ns1").app("app1", "1.0").mr("mr1")
        .run(RunIds.generate());
    Assert.assertEquals(Decision.ACCEPT,
        validator.validate(TopicKind.LOGS,
            encodeLogViaAppender(LoggingContextHelper.getLoggingContextWithRunId(topMrRun, null)),
            topMrRun));

    ProgramRunId workerRun = new NamespaceId("ns1").app("app1", "1.0").worker("wrk1")
        .run(RunIds.generate());
    Assert.assertEquals(Decision.ACCEPT,
        validator.validate(TopicKind.LOGS,
            encodeLogViaAppender(LoggingContextHelper.getLoggingContextWithRunId(workerRun, null)),
            workerRun));

    ProgramRunId serviceRun = new NamespaceId("ns1").app("app1", "1.0").service("svc1")
        .run(RunIds.generate());
    LoggingContext userSvcCtx = new UserServiceLoggingContext(
        "ns1", "app1", "svc1", "handler1", serviceRun.getRun(), "0");
    Assert.assertEquals(Decision.ACCEPT,
        validator.validate(TopicKind.LOGS, encodeLogViaAppender(userSvcCtx), serviceRun));

    // UserService MDC without handlerId -> LoggingContextHelper returns null -> dropped
    byte[] svcMissingHandler = encodeRawLogMdc(ImmutableMap.of(
        NamespaceLoggingContext.TAG_NAMESPACE_ID, "ns1",
        ApplicationLoggingContext.TAG_APPLICATION_ID, "app1",
        UserServiceLoggingContext.TAG_USER_SERVICE_ID, "svc1",
        ApplicationLoggingContext.TAG_RUN_ID, serviceRun.getRun()));
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.LOGS, svcMissingHandler, serviceRun));

    // 4. Vary one identity field at a time while keeping workflowRun's exact run id
    ProgramRunId wrongNsSameRun = new NamespaceId("ns2").app("app1", "1.0").workflow("wf1")
        .run(workflowRun.getRun());
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.LOGS,
            encodeLogViaAppender(
                LoggingContextHelper.getLoggingContextWithRunId(wrongNsSameRun, null)),
            workflowRun));

    ProgramRunId wrongAppSameRun = new NamespaceId("ns1").app("otherApp", "1.0").workflow("wf1")
        .run(workflowRun.getRun());
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.LOGS,
            encodeLogViaAppender(
                LoggingContextHelper.getLoggingContextWithRunId(wrongAppSameRun, null)),
            workflowRun));

    ProgramRunId wrongProgramSameRun = new NamespaceId("ns1").app("app1", "1.0")
        .workflow("otherWf").run(workflowRun.getRun());
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.LOGS,
            encodeLogViaAppender(
                LoggingContextHelper.getLoggingContextWithRunId(wrongProgramSameRun, null)),
            workflowRun));

    ProgramRunId wrongTypeSameNameAndRun = new NamespaceId("ns1").app("app1", "1.0").spark("wf1")
        .run(workflowRun.getRun());
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.LOGS,
            encodeLogViaAppender(
                LoggingContextHelper.getLoggingContextWithRunId(wrongTypeSameNameAndRun, null)),
            workflowRun));

    ProgramRunId wrongRunIdOnly = new NamespaceId("ns1").app("app1", "1.0").workflow("wf1")
        .run(RunIds.generate());
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.LOGS,
            encodeLogViaAppender(
                LoggingContextHelper.getLoggingContextWithRunId(wrongRunIdOnly, null)),
            workflowRun));

    // 5. Extra/conflicting identity MDC tags alongside a valid workflow context -> dropped
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.LOGS,
            encodeLogViaAppender(wfCtx, ImmutableMap.of(
                MapReduceLoggingContext.TAG_MAP_REDUCE_JOB_ID, "otherMr")),
            workflowRun));
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.LOGS,
            encodeLogViaAppender(wfCtx, ImmutableMap.of(
                MapReduceLoggingContext.TAG_MAP_REDUCE_JOB_ID, "../../bad")),
            workflowRun));
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.LOGS,
            encodeLogViaAppender(wfCtx, ImmutableMap.of(".accountId", "developer")),
            workflowRun));
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.LOGS,
            encodeRawLogMdc(ImmutableMap.of(
                ServiceLoggingContext.TAG_SYSTEM_ID, "ns1",
                ApplicationLoggingContext.TAG_APPLICATION_ID, "app1",
                WorkflowLoggingContext.TAG_WORKFLOW_ID, "wf1",
                ApplicationLoggingContext.TAG_RUN_ID, workflowRun.getRun())),
            workflowRun));
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.LOGS,
            encodeLogViaAppender(innerMrCtx, ImmutableMap.of(
                WorkflowProgramLoggingContext.TAG_WORKFLOW_SPARK_ID, "extraSpark")),
            workflowRun));

    // Malformed inner workflow program tags (invalid program name or non-v1-UUID inner run id)
    byte[] badInnerProgram = encodeRawLogMdc(ImmutableMap.<String, String>builder()
        .put(NamespaceLoggingContext.TAG_NAMESPACE_ID, "ns1")
        .put(ApplicationLoggingContext.TAG_APPLICATION_ID, "app1")
        .put(WorkflowLoggingContext.TAG_WORKFLOW_ID, "wf1")
        .put(ApplicationLoggingContext.TAG_RUN_ID, workflowRun.getRun())
        .put(WorkflowProgramLoggingContext.TAG_WORKFLOW_SPARK_ID, "../bad")
        .put(WorkflowProgramLoggingContext.TAG_WORKFLOW_PROGRAM_RUN_ID, innerSparkRun.getRun())
        .build());
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.LOGS, badInnerProgram, workflowRun));

    byte[] badInnerRun = encodeRawLogMdc(ImmutableMap.<String, String>builder()
        .put(NamespaceLoggingContext.TAG_NAMESPACE_ID, "ns1")
        .put(ApplicationLoggingContext.TAG_APPLICATION_ID, "app1")
        .put(WorkflowLoggingContext.TAG_WORKFLOW_ID, "wf1")
        .put(ApplicationLoggingContext.TAG_RUN_ID, workflowRun.getRun())
        .put(WorkflowProgramLoggingContext.TAG_WORKFLOW_SPARK_ID, "innerSpark")
        .put(WorkflowProgramLoggingContext.TAG_WORKFLOW_PROGRAM_RUN_ID,
            UUID.randomUUID().toString())
        .build());
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.LOGS, badInnerRun, workflowRun));

    // 6. System-service context, empty MDC, and undecodable bytes -> dropped
    LoggingContext sysCtx = new ServiceLoggingContext("system", "appfabric", "runtime");
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.LOGS, encodeLogViaAppender(sysCtx), workflowRun));
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.LOGS, encodeRawLogMdc(Collections.emptyMap()), workflowRun));
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.LOGS, "not-avro".getBytes(StandardCharsets.UTF_8),
            workflowRun));
  }

  @Test
  public void testMetricsValidation() {
    // 1. Top-level run tag passes
    byte[] runMetric = encodeMetricViaService(ImmutableMap.of(
        Constants.Metrics.Tag.NAMESPACE, "ns1",
        Constants.Metrics.Tag.APP, "app1",
        Constants.Metrics.Tag.RUN_ID, workflowRun.getRun()));
    Assert.assertEquals(Decision.ACCEPT,
        validator.validate(TopicKind.METRICS, runMetric, workflowRun));

    // 2. Workflow's own wfr-only metric and inner program's wfr+run metric pass for workflowRun
    byte[] wfOnlyMetric = encodeMetricViaService(ImmutableMap.of(
        Constants.Metrics.Tag.NAMESPACE, "ns1",
        Constants.Metrics.Tag.APP, "app1",
        Constants.Metrics.Tag.WORKFLOW_RUN_ID, workflowRun.getRun()));
    Assert.assertEquals(Decision.ACCEPT,
        validator.validate(TopicKind.METRICS, wfOnlyMetric, workflowRun));

    byte[] wfrMetric = encodeMetricViaService(ImmutableMap.of(
        Constants.Metrics.Tag.NAMESPACE, "ns1",
        Constants.Metrics.Tag.APP, "app1",
        Constants.Metrics.Tag.WORKFLOW_RUN_ID, workflowRun.getRun(),
        Constants.Metrics.Tag.RUN_ID, innerSparkRun.getRun()));
    Assert.assertEquals(Decision.ACCEPT,
        validator.validate(TopicKind.METRICS, wfrMetric, workflowRun));

    // 3. Mismatched namespace, application, or run id -> dropped and counted
    byte[] wrongNsMetric = encodeMetricViaService(ImmutableMap.of(
        Constants.Metrics.Tag.NAMESPACE, "ns2",
        Constants.Metrics.Tag.APP, "app1",
        Constants.Metrics.Tag.RUN_ID, workflowRun.getRun()));
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.METRICS, wrongNsMetric, workflowRun));

    byte[] wrongAppMetric = encodeMetricViaService(ImmutableMap.of(
        Constants.Metrics.Tag.NAMESPACE, "ns1",
        Constants.Metrics.Tag.APP, "otherApp",
        Constants.Metrics.Tag.RUN_ID, workflowRun.getRun()));
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.METRICS, wrongAppMetric, workflowRun));

    byte[] wrongRunMetric = encodeMetricViaService(ImmutableMap.of(
        Constants.Metrics.Tag.NAMESPACE, "ns1",
        Constants.Metrics.Tag.APP, "app1",
        Constants.Metrics.Tag.RUN_ID, sparkRun.getRun()));
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.METRICS, wrongRunMetric, workflowRun));

    // 4. Conflicting run + foreign wfr, wfr on non-workflow token, or server-only tags -> dropped
    byte[] foreignWfrWithValidRun = encodeMetricViaService(ImmutableMap.of(
        Constants.Metrics.Tag.NAMESPACE, "ns1",
        Constants.Metrics.Tag.APP, "app1",
        Constants.Metrics.Tag.RUN_ID, workflowRun.getRun(),
        Constants.Metrics.Tag.WORKFLOW_RUN_ID, otherAppWorkflowRun.getRun()));
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.METRICS, foreignWfrWithValidRun, workflowRun));

    byte[] wfrFromNonWorkflow = encodeMetricViaService(ImmutableMap.of(
        Constants.Metrics.Tag.NAMESPACE, "ns1",
        Constants.Metrics.Tag.APP, "app1",
        Constants.Metrics.Tag.WORKFLOW_RUN_ID, sparkRun.getRun(),
        Constants.Metrics.Tag.RUN_ID, innerSparkRun.getRun()));
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.METRICS, wfrFromNonWorkflow, sparkRun));

    // ProfileMetricService runs inside every remote job and emits profile tags for the run.
    ProfileId dataprocProfile = new ProfileId(NamespaceId.SYSTEM.getNamespace(), "dataproc");
    byte[] workflowProfileMetric = encodeProfileMetricViaService(workflowRun, dataprocProfile);
    Assert.assertEquals(Decision.ACCEPT,
        validator.validate(TopicKind.METRICS, workflowProfileMetric, workflowRun));
    byte[] sparkProfileMetric = encodeProfileMetricViaService(sparkRun, dataprocProfile);
    Assert.assertEquals(Decision.ACCEPT,
        validator.validate(TopicKind.METRICS, sparkProfileMetric, sparkRun));
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.METRICS, sparkProfileMetric, workflowRun));

    byte[] withComponentTag = encodeMetricViaService(ImmutableMap.of(
        Constants.Metrics.Tag.NAMESPACE, "ns1",
        Constants.Metrics.Tag.APP, "app1",
        Constants.Metrics.Tag.RUN_ID, workflowRun.getRun(),
        Constants.Metrics.Tag.COMPONENT, Constants.Service.APP_FABRIC_HTTP));
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.METRICS, withComponentTag, workflowRun));

    // 5. Partial-tag shapes (carrying any of app / run / wfr alone) -> DROP_AND_COUNT, not silent
    byte[] appOnly = encodeMetricViaService(ImmutableMap.of(
        Constants.Metrics.Tag.NAMESPACE, "ns1",
        Constants.Metrics.Tag.APP, "app1"));
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.METRICS, appOnly, workflowRun));

    byte[] runOnly = encodeMetricViaService(ImmutableMap.of(
        Constants.Metrics.Tag.NAMESPACE, "ns1",
        Constants.Metrics.Tag.RUN_ID, workflowRun.getRun()));
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.METRICS, runOnly, workflowRun));

    byte[] wfrOnlyNoApp = encodeMetricViaService(ImmutableMap.of(
        Constants.Metrics.Tag.NAMESPACE, "ns1",
        Constants.Metrics.Tag.WORKFLOW_RUN_ID, workflowRun.getRun()));
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.METRICS, wfrOnlyNoApp, workflowRun));

    // 6. Embedded cluster-service metric (neither app nor run/wfr tag) -> dropped silently
    byte[] embeddedMetric = encodeMetricViaService(ImmutableMap.of(
        Constants.Metrics.Tag.NAMESPACE, NamespaceId.SYSTEM.getNamespace(),
        Constants.Metrics.Tag.COMPONENT, Constants.Service.MESSAGING_SERVICE));
    Assert.assertEquals(Decision.DROP_SILENTLY,
        validator.validate(TopicKind.METRICS, embeddedMetric, workflowRun));

    // 7. Undecodable bytes -> dropped and counted
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.METRICS, new byte[]{1, 2, 3}, workflowRun));
  }

  @Test
  public void testMetadataWorkflowLineageUsageAndEmitters() {
    // 1. WORKFLOW_TOKEN: valid for workflowRun; rejected for non-workflow, other version, and
    //    other runs in the same app and version
    BasicWorkflowToken token = new BasicWorkflowToken(1);
    token.setCurrentNode("n1");
    token.put("k", "v");
    MessagingWorkflowStateWriter wfWriter =
        publisherInjector.getInstance(MessagingWorkflowStateWriter.class);
    capturedPayloads.clear();
    wfWriter.setWorkflowToken(workflowRun, token);
    byte[] wfTokenBytes = takeSinglePayload();
    Assert.assertEquals(Decision.ACCEPT,
        validator.validate(TopicKind.METADATA, wfTokenBytes, workflowRun));
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.METADATA, wfTokenBytes, sparkRun));

    // Another run of the same workflow (same ns, app, version) -> rejected by !tokenRunId.equals
    ProgramRunId siblingWorkflowRun = new NamespaceId("ns1").app("app1", "1.0").workflow("wf1")
        .run(RunIds.generate());
    capturedPayloads.clear();
    wfWriter.setWorkflowToken(siblingWorkflowRun, token);
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.METADATA, takeSinglePayload(), workflowRun));

    // Inner run of the same app/version emitting WORKFLOW_TOKEN -> rejected
    capturedPayloads.clear();
    wfWriter.setWorkflowToken(innerSparkRun, token);
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.METADATA, takeSinglePayload(), workflowRun));

    // Workflow token emitted with workflowRun's run id under another application version -> dropped
    capturedPayloads.clear();
    wfWriter.setWorkflowToken(otherVersionWorkflowRun, token);
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.METADATA, takeSinglePayload(), workflowRun));

    // Workflow token emitted by a non-workflow token run for itself -> dropped
    capturedPayloads.clear();
    wfWriter.setWorkflowToken(sparkRun, token);
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.METADATA, takeSinglePayload(), sparkRun));

    // 2. WORKFLOW_STATE: valid with nodeId; dropped for non-workflow, sibling run, or missing node
    capturedPayloads.clear();
    wfWriter.addWorkflowNodeState(workflowRun, new WorkflowNodeStateDetail("n1", NodeStatus.RUNNING));
    Assert.assertEquals(Decision.ACCEPT,
        validator.validate(TopicKind.METADATA, takeSinglePayload(), workflowRun));

    capturedPayloads.clear();
    wfWriter.addWorkflowNodeState(siblingWorkflowRun,
        new WorkflowNodeStateDetail("n1", NodeStatus.RUNNING));
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.METADATA, takeSinglePayload(), workflowRun));

    capturedPayloads.clear();
    wfWriter.addWorkflowNodeState(sparkRun, new WorkflowNodeStateDetail("n1", NodeStatus.RUNNING));
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.METADATA, takeSinglePayload(), sparkRun));

    capturedPayloads.clear();
    wfWriter.addWorkflowNodeState(workflowRun, new WorkflowNodeStateDetail(null, NodeStatus.RUNNING));
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.METADATA, takeSinglePayload(), workflowRun));

    capturedPayloads.clear();
    wfWriter.addWorkflowNodeState(workflowRun, new WorkflowNodeStateDetail("", NodeStatus.RUNNING));
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.METADATA, takeSinglePayload(), workflowRun));

    // 3. LINEAGE: dataset in token run's namespace passes (including inner run); others drop
    MessagingLineageWriter lineageWriter =
        publisherInjector.getInstance(MessagingLineageWriter.class);
    DatasetId validDs = new NamespaceId("ns1").dataset("ds1");

    capturedPayloads.clear();
    lineageWriter.addAccess(innerSparkRun, validDs, AccessType.READ, null);
    Assert.assertEquals(Decision.ACCEPT,
        validator.validate(TopicKind.METADATA, takeSinglePayload(), workflowRun));

    DatasetId otherNsDs = new NamespaceId("ns2").dataset("ds1");
    capturedPayloads.clear();
    lineageWriter.addAccess(workflowRun, otherNsDs, AccessType.READ, null);
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.METADATA, takeSinglePayload(), workflowRun));

    capturedPayloads.clear();
    lineageWriter.addAccess(otherAppWorkflowRun, validDs, AccessType.READ, null);
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.METADATA, takeSinglePayload(), workflowRun));

    capturedPayloads.clear();
    lineageWriter.addAccess(otherVersionWorkflowRun, validDs, AccessType.READ, null);
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.METADATA, takeSinglePayload(), workflowRun));

    // Omitted and malformed Gson datasetId in lineage payload -> dropped
    byte[] missingLineageDs = GSON.toJson(new MetadataMessage(
        MetadataMessage.Type.LINEAGE, workflowRun, new JsonObject()))
        .getBytes(StandardCharsets.UTF_8);
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.METADATA, missingLineageDs, workflowRun));

    JsonObject badDsObj = new JsonObject();
    badDsObj.addProperty("namespace", "ns1");
    badDsObj.addProperty("dataset", "../bad");
    badDsObj.addProperty("entity", "DATASET");
    JsonObject badLineageDsPayload = new JsonObject();
    badLineageDsPayload.add("datasetId", badDsObj);
    byte[] malformedLineageDs = GSON.toJson(new MetadataMessage(
        MetadataMessage.Type.LINEAGE, workflowRun, badLineageDsPayload))
        .getBytes(StandardCharsets.UTF_8);
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.METADATA, malformedLineageDs, workflowRun));

    // 4. USAGE: program in token run's app + dataset in namespace passes; wrong ns/version drop
    MessagingUsageWriter usageWriter = publisherInjector.getInstance(MessagingUsageWriter.class);
    ProgramId innerSparkProgram = innerSparkRun.getParent();
    capturedPayloads.clear();
    usageWriter.register(innerSparkProgram, validDs);
    Assert.assertEquals(Decision.ACCEPT,
        validator.validate(TopicKind.METADATA, takeSinglePayload(), workflowRun));

    capturedPayloads.clear();
    usageWriter.register(innerSparkProgram, otherNsDs);
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.METADATA, takeSinglePayload(), workflowRun));

    capturedPayloads.clear();
    usageWriter.register(otherVersionWorkflowRun.getParent(), validDs);
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.METADATA, takeSinglePayload(), workflowRun));

    byte[] missingUsageDs = GSON.toJson(new MetadataMessage(
        MetadataMessage.Type.USAGE, innerSparkProgram, new JsonObject()))
        .getBytes(StandardCharsets.UTF_8);
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.METADATA, missingUsageDs, workflowRun));

    // 5. No emitter, non-program/run emitter (ApplicationId), malformed run id, disallowed type
    byte[] noEmitter = GSON.toJson(new MetadataMessage(
        MetadataMessage.Type.WORKFLOW_TOKEN, null, GSON.toJsonTree(token)))
        .getBytes(StandardCharsets.UTF_8);
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.METADATA, noEmitter, workflowRun));

    byte[] appEmitter = GSON.toJson(new MetadataMessage(
        MetadataMessage.Type.USAGE, workflowRun.getParent().getParent(),
        GSON.toJsonTree(Collections.singletonMap("datasetId", validDs))))
        .getBytes(StandardCharsets.UTF_8);
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.METADATA, appEmitter, workflowRun));

    ProgramRunId v4RunId = new NamespaceId("ns1").app("app1", "1.0").spark("s")
        .run(UUID.randomUUID().toString());
    capturedPayloads.clear();
    lineageWriter.addAccess(v4RunId, validDs, AccessType.READ, null);
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.METADATA, takeSinglePayload(), workflowRun));

    byte[] profileMsg = GSON.toJson(new MetadataMessage(
        MetadataMessage.Type.PROFILE_ASSIGNMENT, workflowRun, new JsonObject()))
        .getBytes(StandardCharsets.UTF_8);
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.METADATA, profileMsg, workflowRun));

    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.METADATA, "{bad-json".getBytes(StandardCharsets.UTF_8),
            workflowRun));
  }

  @Test
  public void testMetadataFieldLineageValidation() {
    MessagingLineageWriter lineageWriter =
        publisherInjector.getInstance(MessagingLineageWriter.class);

    ReadOperation readOp = new ReadOperation("read", "read desc",
        EndPoint.of("ns1", "src"), "a", "b", "dropped");
    TransformOperation parseOp = new TransformOperation("parse", "parse desc",
        Collections.singletonList(InputField.of("read", "a")), "out");
    // Drop transform (empty outputs) exercises FieldLineageInfo.NULL_EPF in outgoingSummary
    TransformOperation dropOp = new TransformOperation("drop", "drop desc",
        Collections.singletonList(InputField.of("read", "dropped")), Collections.emptyList());
    WriteOperation writeOp = new WriteOperation("write", "write desc",
        EndPoint.of("ns1", "dst"),
        Arrays.asList(InputField.of("parse", "out"), InputField.of("read", "b")));
    FieldLineageInfo validInfo =
        new FieldLineageInfo(Arrays.asList(readOp, parseOp, dropOp, writeOp));

    capturedPayloads.clear();
    lineageWriter.write(innerSparkRun, validInfo);
    byte[] validBytes = takeSinglePayload();
    Assert.assertEquals(Decision.ACCEPT,
        validator.validate(TopicKind.METADATA, validBytes, workflowRun));

    // Endpoint in another namespace, with no namespace, or with no name -> dropped
    FieldLineageInfo otherNsInfo = new FieldLineageInfo(Arrays.asList(
        new ReadOperation("read", "d", EndPoint.of("ns2", "src"), "a"),
        new WriteOperation("write", "d", EndPoint.of("ns1", "dst"),
            Collections.singletonList(InputField.of("read", "a")))));
    capturedPayloads.clear();
    lineageWriter.write(workflowRun, otherNsInfo);
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.METADATA, takeSinglePayload(), workflowRun));

    FieldLineageInfo noNsInfo = new FieldLineageInfo(Arrays.asList(
        new ReadOperation("read", "d", EndPoint.of("src"), "a"),
        new WriteOperation("write", "d", EndPoint.of("ns1", "dst"),
            Collections.singletonList(InputField.of("read", "a")))));
    capturedPayloads.clear();
    lineageWriter.write(workflowRun, noNsInfo);
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.METADATA, takeSinglePayload(), workflowRun));

    FieldLineageInfo noNameInfo = new FieldLineageInfo(Arrays.asList(
        new ReadOperation("read", "d", EndPoint.of("ns1", "src"), "a"),
        new WriteOperation("write", "d", EndPoint.of("ns1", ""),
            Collections.singletonList(InputField.of("read", "a")))));
    capturedPayloads.clear();
    lineageWriter.write(workflowRun, noNameInfo);
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.METADATA, takeSinglePayload(), workflowRun));

    // Changed checksum -> dropped
    String validJson = new String(validBytes, StandardCharsets.UTF_8);
    JsonObject changedChecksum = new JsonParser().parse(validJson).getAsJsonObject();
    changedChecksum.getAsJsonObject("payload")
        .addProperty("checksum", validInfo.getChecksum() + 1L);
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.METADATA,
            changedChecksum.toString().getBytes(StandardCharsets.UTF_8), workflowRun));

    // Changed, omitted, or null destinationFields, incomingSummary, and outgoingSummary -> dropped
    for (String field : Arrays.asList("destinationFields", "incomingSummary", "outgoingSummary")) {
      JsonObject modified = new JsonParser().parse(validJson).getAsJsonObject();
      modified.getAsJsonObject("payload").add(field, new JsonArray());
      Assert.assertEquals("Modified " + field + " must be dropped", Decision.DROP_AND_COUNT,
          validator.validate(TopicKind.METADATA,
              modified.toString().getBytes(StandardCharsets.UTF_8), workflowRun));

      JsonObject omitted = new JsonParser().parse(validJson).getAsJsonObject();
      omitted.getAsJsonObject("payload").remove(field);
      Assert.assertEquals("Omitted " + field + " must be dropped", Decision.DROP_AND_COUNT,
          validator.validate(TopicKind.METADATA,
              omitted.toString().getBytes(StandardCharsets.UTF_8), workflowRun));

      JsonObject nullField = new JsonParser().parse(validJson).getAsJsonObject();
      nullField.getAsJsonObject("payload").add(field, JsonNull.INSTANCE);
      Assert.assertEquals("Null " + field + " must be dropped", Decision.DROP_AND_COUNT,
          validator.validate(TopicKind.METADATA,
              nullField.toString().getBytes(StandardCharsets.UTF_8), workflowRun));
    }

    // Cycle between two transforms -> dropped cleanly via iterative cycle check
    ReadOperation cycRead = new ReadOperation("r", "d", EndPoint.of("ns1", "src"), "x");
    TransformOperation cycT1 = new TransformOperation("t1", "d",
        Arrays.asList(InputField.of("r", "x"), InputField.of("t2", "x")), "x");
    TransformOperation cycT2 = new TransformOperation("t2", "d",
        Collections.singletonList(InputField.of("t1", "x")), "x");
    WriteOperation cycWrite = new WriteOperation("w", "d", EndPoint.of("ns1", "dst"),
        Collections.singletonList(InputField.of("t2", "x")));
    JsonObject cyclePayload = new JsonObject();
    cyclePayload.addProperty("checksum", 123L);
    cyclePayload.add("operations",
        OPERATION_GSON.toJsonTree(Arrays.asList(cycRead, cycT1, cycT2, cycWrite)));
    cyclePayload.add("destinationFields", new JsonArray());
    cyclePayload.add("incomingSummary", new JsonArray());
    cyclePayload.add("outgoingSummary", new JsonArray());
    byte[] cycleBytes = GSON.toJson(new MetadataMessage(
        MetadataMessage.Type.FIELD_LINEAGE, workflowRun, cyclePayload))
        .getBytes(StandardCharsets.UTF_8);
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.METADATA, cycleBytes, workflowRun));

    // Deep linear chain enough to overflow stack -> verify StackOverflowError occurs directly and
    // is caught by validate() as Throwable so only this payload is dropped
    List<Operation> deepOps = new ArrayList<>();
    deepOps.add(new ReadOperation("op0", "d", EndPoint.of("ns1", "src"), "f"));
    int depth = 15000;
    for (int i = 1; i <= depth; i++) {
      deepOps.add(new TransformOperation("op" + i, "d",
          Collections.singletonList(InputField.of("op" + (i - 1), "f")), "f"));
    }
    deepOps.add(new WriteOperation("opWrite", "d", EndPoint.of("ns1", "dst"),
        Collections.singletonList(InputField.of("op" + depth, "f"))));
    try {
      new FieldLineageInfo(deepOps);
      Assert.fail("Expected StackOverflowError for chain of depth " + depth);
    } catch (StackOverflowError expected) {
      // Expected: proves the chain overflows the stack rather than failing checksum comparison.
    }
    JsonObject deepPayload = new JsonObject();
    deepPayload.addProperty("checksum", 1L);
    deepPayload.add("operations", OPERATION_GSON.toJsonTree(deepOps));
    deepPayload.add("destinationFields", new JsonArray());
    deepPayload.add("incomingSummary", new JsonArray());
    deepPayload.add("outgoingSummary", new JsonArray());
    byte[] deepBytes = GSON.toJson(new MetadataMessage(
        MetadataMessage.Type.FIELD_LINEAGE, workflowRun, deepPayload))
        .getBytes(StandardCharsets.UTF_8);
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.METADATA, deepBytes, workflowRun));
  }

  @Test
  public void testMetadataOperationValidation() {
    MessagingMetadataPublisher publisher =
        publisherInjector.getInstance(MessagingMetadataPublisher.class);
    MetadataEntity validEntity = MetadataEntity.ofDataset("ns1", "ds1");
    MetadataEntity validCustomField = MetadataEntity.builder()
        .append(MetadataEntity.NAMESPACE, "ns1")
        .append(MetadataEntity.DATASET, "ds1")
        .appendAsType("field", "col1")
        .build();
    MetadataEntity validPlugin = new PluginId("ns1", "art", "1.0", "myPlugin", "batchsource")
        .toMetadataEntity();
    MetadataEntity otherNsEntity = MetadataEntity.ofDataset("ns2", "ds1");

    // All USER-scoped PUT and DELETE* operations in token run's namespace pass
    List<MetadataOperation> allowedOps = Arrays.asList(
        new MetadataOperation.Put(validEntity, MetadataScope.USER,
            ImmutableMap.of("k", "v"), ImmutableSet.of("t")),
        new MetadataOperation.Put(validCustomField, MetadataScope.USER,
            ImmutableMap.of("k", "v"), ImmutableSet.of("t")),
        new MetadataOperation.Put(validPlugin, MetadataScope.USER,
            ImmutableMap.of("k", "v"), ImmutableSet.of("t")),
        new MetadataOperation.Delete(validEntity, MetadataScope.USER,
            ImmutableSet.of("k"), ImmutableSet.of("t")),
        new MetadataOperation.DeleteAll(validEntity, MetadataScope.USER),
        new MetadataOperation.DeleteAllProperties(validEntity, MetadataScope.USER),
        new MetadataOperation.DeleteAllTags(validEntity, MetadataScope.USER));
    for (MetadataOperation op : allowedOps) {
      capturedPayloads.clear();
      publisher.publish(innerSparkRun, op);
      Assert.assertEquals("Expected ACCEPT for " + op.getType(), Decision.ACCEPT,
          validator.validate(TopicKind.METADATA, takeSinglePayload(), workflowRun));
    }

    // SYSTEM scope, CREATE, DROP, other namespace, invalid entity name, and keys before namespace
    MetadataEntity invalidDsName = MetadataEntity.ofDataset("ns1", "bad/name!@#");
    MetadataEntity keysBeforeNamespace = MetadataEntity.builder()
        .append("foo", "ns2")
        .append("bar", "otherArtifact")
        .append(MetadataEntity.VERSION, "1.0")
        .append(MetadataEntity.NAMESPACE, "ns1")
        .append(MetadataEntity.ARTIFACT, "x")
        .appendAsType("custom", "y")
        .build();
    List<MetadataOperation> deniedOps = Arrays.asList(
        new MetadataOperation.Put(validEntity, MetadataScope.SYSTEM,
            ImmutableMap.of("k", "v"), ImmutableSet.of("t")),
        new MetadataOperation.Delete(validEntity, MetadataScope.SYSTEM,
            ImmutableSet.of("k"), ImmutableSet.of("t")),
        new MetadataOperation.DeleteAll(validEntity, MetadataScope.SYSTEM),
        new MetadataOperation.DeleteAllProperties(validEntity, MetadataScope.SYSTEM),
        new MetadataOperation.DeleteAllTags(validEntity, MetadataScope.SYSTEM),
        new MetadataOperation.Create(validEntity, ImmutableMap.of("k", "v"), ImmutableSet.of("t")),
        new MetadataOperation.Drop(validEntity),
        new MetadataOperation.Put(otherNsEntity, MetadataScope.USER,
            ImmutableMap.of("k", "v"), ImmutableSet.of("t")),
        new MetadataOperation.Put(invalidDsName, MetadataScope.USER,
            ImmutableMap.of("k", "v"), ImmutableSet.of("t")),
        new MetadataOperation.Put(keysBeforeNamespace, MetadataScope.USER,
            ImmutableMap.of("k", "v"), ImmutableSet.of("t")));
    for (MetadataOperation op : deniedOps) {
      capturedPayloads.clear();
      publisher.publish(workflowRun, op);
      Assert.assertEquals("Expected DROP for " + op, Decision.DROP_AND_COUNT,
          validator.validate(TopicKind.METADATA, takeSinglePayload(), workflowRun));
    }

    // Malformed MetadataEntity cases: no type, unknown type, null value, upper-case NAMESPACE,
    // reversed CDAP hierarchy keys
    capturedPayloads.clear();
    publisher.publish(workflowRun, new MetadataOperation.DeleteAll(validEntity));
    String baseJson = new String(takeSinglePayload(), StandardCharsets.UTF_8);

    JsonObject noType = new JsonParser().parse(baseJson).getAsJsonObject();
    noType.getAsJsonObject("payload").getAsJsonObject("entity").remove("type");
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.METADATA, noType.toString().getBytes(StandardCharsets.UTF_8),
            workflowRun));

    JsonObject missingTypeKey = new JsonParser().parse(baseJson).getAsJsonObject();
    missingTypeKey.getAsJsonObject("payload").getAsJsonObject("entity")
        .addProperty("type", "bogus");
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.METADATA,
            missingTypeKey.toString().getBytes(StandardCharsets.UTF_8), workflowRun));

    JsonObject nullValue = new JsonParser().parse(baseJson).getAsJsonObject();
    nullValue.getAsJsonObject("payload").getAsJsonObject("entity")
        .getAsJsonObject("details").add("dataset", JsonNull.INSTANCE);
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.METADATA,
            nullValue.toString().getBytes(StandardCharsets.UTF_8), workflowRun));

    JsonObject upperCaseNs = new JsonParser().parse(baseJson).getAsJsonObject();
    JsonObject upperDetails = new JsonObject();
    upperDetails.addProperty("NAMESPACE", "ns1");
    upperDetails.addProperty("dataset", "ds1");
    upperCaseNs.getAsJsonObject("payload").getAsJsonObject("entity").add("details", upperDetails);
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.METADATA,
            upperCaseNs.toString().getBytes(StandardCharsets.UTF_8), workflowRun));

    JsonObject reversedKeys = new JsonParser().parse(baseJson).getAsJsonObject();
    JsonObject revDetails = new JsonObject();
    revDetails.addProperty("dataset", "ds1");
    revDetails.addProperty("namespace", "ns1");
    reversedKeys.getAsJsonObject("payload").getAsJsonObject("entity").add("details", revDetails);
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.METADATA,
            reversedKeys.toString().getBytes(StandardCharsets.UTF_8), workflowRun));
  }

  @Test
  public void testAuditValidation() {
    DefaultAuditPublisher auditPublisher =
        publisherInjector.getInstance(DefaultAuditPublisher.class);
    DatasetId validDs = new NamespaceId("ns1").dataset("ds1");

    // 1. ACCESS record of token run's program / inner run in its namespace -> passes
    capturedPayloads.clear();
    auditPublisher.publish(validDs, AuditType.ACCESS,
        new AccessPayload(io.cdap.cdap.proto.audit.payload.access.AccessType.READ,
            innerSparkRun.getParent()));
    byte[] validAuditBytes = takeSinglePayload();
    Assert.assertEquals(Decision.ACCEPT,
        validator.validate(TopicKind.AUDIT, validAuditBytes, workflowRun));

    capturedPayloads.clear();
    auditPublisher.publish(validDs, AuditType.ACCESS,
        new AccessPayload(io.cdap.cdap.proto.audit.payload.access.AccessType.WRITE, workflowRun));
    Assert.assertEquals(Decision.ACCEPT,
        validator.validate(TopicKind.AUDIT, takeSinglePayload(), workflowRun));

    // 2. Non-ACCESS type -> dropped
    capturedPayloads.clear();
    auditPublisher.publish(validDs, AuditType.CREATE, AuditPayload.EMPTY_PAYLOAD);
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.AUDIT, takeSinglePayload(), workflowRun));

    // 3. No payload / no entity -> dropped
    String validAuditJson = new String(validAuditBytes, StandardCharsets.UTF_8);
    JsonObject noPayload = new JsonParser().parse(validAuditJson).getAsJsonObject();
    noPayload.remove("payload");
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.AUDIT,
            noPayload.toString().getBytes(StandardCharsets.UTF_8), workflowRun));

    JsonObject noEntity = new JsonParser().parse(validAuditJson).getAsJsonObject();
    noEntity.remove("metadataEntity");
    noEntity.remove("entityId");
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.AUDIT,
            noEntity.toString().getBytes(StandardCharsets.UTF_8), workflowRun));

    // 4. Malformed v1 entityId (invalid dataset name, '..' version, non-v1 UUID) -> dropped
    JsonObject badV1Dataset = new JsonParser().parse(validAuditJson).getAsJsonObject();
    badV1Dataset.remove("metadataEntity");
    JsonObject v1Ds = new JsonObject();
    v1Ds.addProperty("namespace", "ns1");
    v1Ds.addProperty("dataset", "../bad");
    v1Ds.addProperty("entity", "DATASET");
    badV1Dataset.add("entityId", v1Ds);
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.AUDIT,
            badV1Dataset.toString().getBytes(StandardCharsets.UTF_8), workflowRun));

    JsonObject badV1App = new JsonParser().parse(validAuditJson).getAsJsonObject();
    badV1App.remove("metadataEntity");
    JsonObject v1App = new JsonObject();
    v1App.addProperty("namespace", "ns1");
    v1App.addProperty("application", "app1");
    v1App.addProperty("version", "..");
    v1App.addProperty("entity", "APPLICATION");
    badV1App.add("entityId", v1App);
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.AUDIT,
            badV1App.toString().getBytes(StandardCharsets.UTF_8), workflowRun));

    JsonObject badV1ProgramRun = new JsonParser().parse(validAuditJson).getAsJsonObject();
    badV1ProgramRun.remove("metadataEntity");
    JsonObject v1Run = new JsonObject();
    v1Run.addProperty("namespace", "ns1");
    v1Run.addProperty("application", "app1");
    v1Run.addProperty("version", "1.0");
    v1Run.addProperty("type", "Workflow");
    v1Run.addProperty("program", "wf1");
    v1Run.addProperty("run", UUID.randomUUID().toString());
    v1Run.addProperty("entity", "PROGRAM_RUN");
    badV1ProgramRun.add("entityId", v1Run);
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.AUDIT,
            badV1ProgramRun.toString().getBytes(StandardCharsets.UTF_8), workflowRun));

    // 5. Foreign accessor, non-program/run accessor (ApplicationId), and wrong version -> dropped
    capturedPayloads.clear();
    auditPublisher.publish(validDs, AuditType.ACCESS,
        new AccessPayload(io.cdap.cdap.proto.audit.payload.access.AccessType.READ,
            otherAppWorkflowRun));
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.AUDIT, takeSinglePayload(), workflowRun));

    capturedPayloads.clear();
    auditPublisher.publish(validDs, AuditType.ACCESS,
        new AccessPayload(io.cdap.cdap.proto.audit.payload.access.AccessType.READ,
            workflowRun.getParent().getParent()));
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.AUDIT, takeSinglePayload(), workflowRun));

    capturedPayloads.clear();
    auditPublisher.publish(validDs, AuditType.ACCESS,
        new AccessPayload(io.cdap.cdap.proto.audit.payload.access.AccessType.READ,
            otherVersionWorkflowRun));
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.AUDIT, takeSinglePayload(), workflowRun));

    // 6. Entity in another namespace -> dropped
    DatasetId otherNsDs = new NamespaceId("ns2").dataset("ds1");
    capturedPayloads.clear();
    auditPublisher.publish(otherNsDs, AuditType.ACCESS,
        new AccessPayload(io.cdap.cdap.proto.audit.payload.access.AccessType.READ, workflowRun));
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.AUDIT, takeSinglePayload(), workflowRun));

    // 7. Undecodable audit payload -> dropped
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.AUDIT, "not-json".getBytes(StandardCharsets.UTF_8),
            workflowRun));
  }

  @Test
  public void testDataEventValidation() {
    PartitionKey key = PartitionKey.builder().addStringField("k", "v").build();
    byte[] validNotification = GSON.toJson(
        Notification.forPartitions(new NamespaceId("ns1").dataset("ds1"),
            Collections.singletonList(key))).getBytes(StandardCharsets.UTF_8);
    Assert.assertEquals(Decision.ACCEPT,
        validator.validate(TopicKind.DATA_EVENT, validNotification, workflowRun));

    // Another namespace -> dropped
    byte[] otherNsNotification = GSON.toJson(
        Notification.forPartitions(new NamespaceId("ns2").dataset("ds1"),
            Collections.singletonList(key))).getBytes(StandardCharsets.UTF_8);
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.DATA_EVENT, otherNsNotification, workflowRun));

    // Non-partition notification -> dropped
    byte[] timeNotification = GSON.toJson(
        new Notification(Notification.Type.TIME,
            ImmutableMap.of(Notification.DATASET_ID, "dataset:ns1.ds1")))
        .getBytes(StandardCharsets.UTF_8);
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.DATA_EVENT, timeNotification, workflowRun));

    // Invalid dataset id string -> dropped
    byte[] badDataset = GSON.toJson(
        new Notification(Notification.Type.PARTITION,
            ImmutableMap.of(Notification.DATASET_ID, "namespace:ns1")))
        .getBytes(StandardCharsets.UTF_8);
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.DATA_EVENT, badDataset, workflowRun));

    // Empty properties, missing properties, and undecodable -> dropped
    byte[] emptyProps = GSON.toJson(
        new Notification(Notification.Type.PARTITION, Collections.emptyMap()))
        .getBytes(StandardCharsets.UTF_8);
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.DATA_EVENT, emptyProps, workflowRun));

    JsonElement noProps = GSON.toJsonTree(new Notification(Notification.Type.PARTITION, null));
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.DATA_EVENT,
            noProps.toString().getBytes(StandardCharsets.UTF_8), workflowRun));
    Assert.assertEquals(Decision.DROP_AND_COUNT,
        validator.validate(TopicKind.DATA_EVENT, "bad".getBytes(StandardCharsets.UTF_8),
            workflowRun));
  }
}
