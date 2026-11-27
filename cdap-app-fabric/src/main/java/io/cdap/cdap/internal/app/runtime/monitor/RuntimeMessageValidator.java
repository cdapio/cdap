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

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.UnsupportedTypeException;
import io.cdap.cdap.api.lineage.field.EndPoint;
import io.cdap.cdap.api.lineage.field.Operation;
import io.cdap.cdap.api.lineage.field.OperationType;
import io.cdap.cdap.api.lineage.field.ReadOperation;
import io.cdap.cdap.api.lineage.field.WriteOperation;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.api.metrics.MetricValues;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.io.BinaryDecoder;
import io.cdap.cdap.common.io.DatumReader;
import io.cdap.cdap.common.logging.ComponentLoggingContext;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.logging.NamespaceLoggingContext;
import io.cdap.cdap.common.logging.ServiceLoggingContext;
import io.cdap.cdap.data2.metadata.lineage.field.EndPointField;
import io.cdap.cdap.data2.metadata.lineage.field.EndpointFieldDeserializer;
import io.cdap.cdap.data2.metadata.lineage.field.FieldLineageInfo;
import io.cdap.cdap.data2.metadata.writer.DataAccessLineage;
import io.cdap.cdap.data2.metadata.writer.MetadataMessage;
import io.cdap.cdap.data2.metadata.writer.MetadataOperation;
import io.cdap.cdap.data2.metadata.writer.MetadataOperationTypeAdapter;
import io.cdap.cdap.data2.registry.DatasetUsage;
import io.cdap.cdap.internal.app.runtime.workflow.BasicWorkflowToken;
import io.cdap.cdap.internal.io.ReflectionDatumReaderFactory;
import io.cdap.cdap.internal.io.ReflectionSchemaGenerator;
import io.cdap.cdap.logging.context.ApplicationLoggingContext;
import io.cdap.cdap.logging.context.LoggingContextHelper;
import io.cdap.cdap.logging.context.MapReduceLoggingContext;
import io.cdap.cdap.logging.context.SparkLoggingContext;
import io.cdap.cdap.logging.context.UserServiceLoggingContext;
import io.cdap.cdap.logging.context.WorkerLoggingContext;
import io.cdap.cdap.logging.context.WorkflowLoggingContext;
import io.cdap.cdap.logging.context.WorkflowProgramLoggingContext;
import io.cdap.cdap.logging.serialize.LoggingEventSerializer;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.WorkflowNodeStateDetail;
import io.cdap.cdap.proto.audit.AuditMessage;
import io.cdap.cdap.proto.audit.AuditType;
import io.cdap.cdap.proto.audit.payload.access.AccessPayload;
import io.cdap.cdap.proto.codec.AuditMessageTypeAdapter;
import io.cdap.cdap.proto.codec.EntityIdTypeAdapter;
import io.cdap.cdap.proto.codec.OperationTypeAdapter;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespacedEntityId;
import io.cdap.cdap.proto.id.PluginId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Validates TMS message payloads relayed by remote program runs authenticated with a
 * {@code RUNTIME} token on logs, metrics, metadata, audit, and data-event topics.
 */
@Singleton
final class RuntimeMessageValidator {

  private static final Gson PLAIN_GSON = new Gson();
  private static final Gson METADATA_GSON = new GsonBuilder()
      .registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())
      .registerTypeAdapter(MetadataOperation.class, new MetadataOperationTypeAdapter())
      .registerTypeAdapter(Operation.class, new OperationTypeAdapter())
      .create();
  private static final Gson AUDIT_GSON = new GsonBuilder()
      .registerTypeAdapter(AuditMessage.class, new AuditMessageTypeAdapter())
      .registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())
      .create();

  private static final Schema METRIC_SCHEMA = generateMetricSchema();
  private static final ThreadLocal<DatumReader<MetricValues>> METRIC_READER =
      ThreadLocal.withInitial(() -> new ReflectionDatumReaderFactory()
          .create(TypeToken.of(MetricValues.class), METRIC_SCHEMA));
  private static final ThreadLocal<LoggingEventSerializer> LOGGING_EVENT_SERIALIZER =
      ThreadLocal.withInitial(LoggingEventSerializer::new);
  private static final Set<String> IDENTITY_MDC_KEYS = ImmutableSet.of(
      NamespaceLoggingContext.TAG_NAMESPACE_ID,
      ServiceLoggingContext.TAG_SYSTEM_ID,
      ".accountId",
      ApplicationLoggingContext.TAG_APPLICATION_ID,
      ComponentLoggingContext.TAG_COMPONENT_ID,
      ServiceLoggingContext.TAG_SERVICE_ID,
      ApplicationLoggingContext.TAG_RUN_ID,
      ApplicationLoggingContext.TAG_INSTANCE_ID,
      WorkflowLoggingContext.TAG_WORKFLOW_ID,
      WorkflowProgramLoggingContext.TAG_WORKFLOW_MAP_REDUCE_ID,
      WorkflowProgramLoggingContext.TAG_WORKFLOW_SPARK_ID,
      WorkflowProgramLoggingContext.TAG_WORKFLOW_PROGRAM_RUN_ID,
      MapReduceLoggingContext.TAG_MAP_REDUCE_JOB_ID,
      SparkLoggingContext.TAG_SPARK_JOB_ID,
      UserServiceLoggingContext.TAG_USER_SERVICE_ID,
      UserServiceLoggingContext.TAG_HANDLER_ID,
      WorkerLoggingContext.TAG_WORKER_ID);

  private final String logsTopicPrefix;
  private final String metricsTopicPrefix;
  private final String metadataTopic;
  private final String auditTopic;
  private final String dataEventTopic;
  private final String programStatusTopic;

  /**
   * Classification of an allowed TMS topic for message validation.
   */
  enum TopicKind {
    LOGS(RuntimeValidationFailure.MESSAGE_LOGS),
    METRICS(RuntimeValidationFailure.MESSAGE_METRICS),
    METADATA(RuntimeValidationFailure.MESSAGE_METADATA),
    AUDIT(RuntimeValidationFailure.MESSAGE_AUDIT),
    DATA_EVENT(RuntimeValidationFailure.MESSAGE_DATA_EVENT),
    PROGRAM_STATUS(null),
    OTHER(RuntimeValidationFailure.MESSAGE_TOPIC);

    @Nullable
    private final RuntimeValidationFailure failure;

    TopicKind(@Nullable RuntimeValidationFailure failure) {
      this.failure = failure;
    }

    @Nullable
    RuntimeValidationFailure getFailure() {
      return failure;
    }
  }

  /**
   * Per-payload validation decision.
   */
  enum Decision {
    ACCEPT,
    DROP_AND_COUNT,
    DROP_SILENTLY
  }

  @Inject
  RuntimeMessageValidator(CConfiguration cConf) {
    this.logsTopicPrefix = cConf.get(Constants.Logging.TMS_TOPIC_PREFIX);
    this.metricsTopicPrefix = cConf.get(Constants.Metrics.TOPIC_PREFIX);
    this.metadataTopic = cConf.get(Constants.Metadata.MESSAGING_TOPIC);
    this.auditTopic = cConf.get(Constants.Audit.TOPIC);
    this.dataEventTopic = cConf.get(Constants.Dataset.DATA_EVENT_TOPIC);
    this.programStatusTopic = cConf.get(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC);
  }

  /**
   * Classifies a TMS topic name (matching numbered topic partitions for logs, metrics, and program
   * status events) into a {@link TopicKind} for payload validation.
   *
   * @param topic the system TMS topic name from the request path
   * @return the corresponding {@link TopicKind}, or {@link TopicKind#OTHER} if unrecognized
   */
  TopicKind classifyTopic(@Nullable String topic) {
    if (Strings.isNullOrEmpty(topic)) {
      return TopicKind.OTHER;
    }
    if (matchesTopicOrPartition(topic, programStatusTopic)) {
      return TopicKind.PROGRAM_STATUS;
    }
    if (matchesTopicOrPartition(topic, logsTopicPrefix)) {
      return TopicKind.LOGS;
    }
    if (matchesTopicOrPartition(topic, metricsTopicPrefix)) {
      return TopicKind.METRICS;
    }
    if (topic.equals(metadataTopic)) {
      return TopicKind.METADATA;
    }
    if (topic.equals(auditTopic)) {
      return TopicKind.AUDIT;
    }
    if (topic.equals(dataEventTopic)) {
      return TopicKind.DATA_EVENT;
    }
    return TopicKind.OTHER;
  }

  /**
   * Validates a single TMS message payload against the authenticated {@code RUNTIME} token run.
   *
   * @param topicKind classification of the target topic
   * @param payload raw message bytes decoded from the Avro request stream
   * @param tokenRunId program run identity from the validated {@code RUNTIME} token
   * @return {@link Decision#ACCEPT} if valid, {@link Decision#DROP_SILENTLY} for cluster-service
   *     metrics without app/run tags, or {@link Decision#DROP_AND_COUNT} otherwise
   */
  Decision validate(@Nullable TopicKind topicKind, @Nullable byte[] payload,
      @Nullable ProgramRunId tokenRunId) {
    if (topicKind == TopicKind.PROGRAM_STATUS) {
      return Decision.ACCEPT;
    }
    if (topicKind == null || topicKind == TopicKind.OTHER
        || payload == null || tokenRunId == null) {
      return Decision.DROP_AND_COUNT;
    }
    try {
      switch (topicKind) {
        case LOGS:
          return validateLog(payload, tokenRunId);
        case METRICS:
          return validateMetric(payload, tokenRunId);
        case METADATA:
          return validateMetadata(payload, tokenRunId);
        case AUDIT:
          return validateAudit(payload, tokenRunId);
        case DATA_EVENT:
          return validateDataEvent(payload, tokenRunId);
        default:
          return Decision.DROP_AND_COUNT;
      }
    } catch (Exception | StackOverflowError e) {
      return Decision.DROP_AND_COUNT;
    }
  }

  private static Decision validateLog(byte[] payload, ProgramRunId tokenRunId) throws IOException {
    ILoggingEvent event = LOGGING_EVENT_SERIALIZER.get().fromBytes(ByteBuffer.wrap(payload));
    if (event == null) {
      return Decision.DROP_AND_COUNT;
    }
    Map<String, String> mdc = event.getMDCPropertyMap();
    LoggingContext context = LoggingContextHelper.getLoggingContext(mdc);
    if (programTypeForContext(context) != tokenRunId.getType()) {
      return Decision.DROP_AND_COUNT;
    }
    Map<String, LoggingContext.SystemTag> tags = context.getSystemTagsMap();
    // Ensure no extra or mismatched identity/routing MDC keys can ride alongside the context
    // into downstream log appenders or readers.
    for (Map.Entry<String, String> entry : mdc.entrySet()) {
      String key = entry.getKey();
      if (IDENTITY_MDC_KEYS.contains(key)) {
        String value = entry.getValue();
        if (value == null || !tagEquals(tags.get(key), value)) {
          return Decision.DROP_AND_COUNT;
        }
      }
    }
    LoggingContext.SystemTag entityTag = LoggingContextHelper.getEntityId(context);
    boolean matches = tagEquals(tags.get(NamespaceLoggingContext.TAG_NAMESPACE_ID),
        tokenRunId.getNamespace())
        && tagEquals(tags.get(ApplicationLoggingContext.TAG_APPLICATION_ID),
        tokenRunId.getApplication())
        && tagEquals(entityTag, tokenRunId.getProgram())
        && tagEquals(tags.get(ApplicationLoggingContext.TAG_RUN_ID), tokenRunId.getRun());
    if (!matches) {
      return Decision.DROP_AND_COUNT;
    }
    if (context instanceof WorkflowProgramLoggingContext
        && !isValidWorkflowProgramTags(tags, tokenRunId)) {
      return Decision.DROP_AND_COUNT;
    }
    return Decision.ACCEPT;
  }

  private static boolean isValidWorkflowProgramTags(Map<String, LoggingContext.SystemTag> tags,
      ProgramRunId tokenRunId) {
    LoggingContext.SystemTag innerRunTag =
        tags.get(WorkflowProgramLoggingContext.TAG_WORKFLOW_PROGRAM_RUN_ID);
    if (innerRunTag == null || Strings.isNullOrEmpty(innerRunTag.getValue())) {
      return false;
    }
    LoggingContext.SystemTag mrTag =
        tags.get(WorkflowProgramLoggingContext.TAG_WORKFLOW_MAP_REDUCE_ID);
    LoggingContext.SystemTag sparkTag =
        tags.get(WorkflowProgramLoggingContext.TAG_WORKFLOW_SPARK_ID);
    ProgramType innerType;
    String innerProgram;
    if (mrTag != null && sparkTag == null) {
      innerType = ProgramType.MAPREDUCE;
      innerProgram = mrTag.getValue();
    } else if (sparkTag != null && mrTag == null) {
      innerType = ProgramType.SPARK;
      innerProgram = sparkTag.getValue();
    } else {
      return false;
    }
    if (Strings.isNullOrEmpty(innerProgram)) {
      return false;
    }
    ProgramRunId innerRun = new ProgramRunId(
        tokenRunId.getParent().getParent(), innerType, innerProgram, innerRunTag.getValue());
    return RuntimeMonitors.isWellFormed(innerRun);
  }

  @Nullable
  private static ProgramType programTypeForContext(@Nullable LoggingContext context) {
    if (context instanceof WorkflowLoggingContext) {
      return ProgramType.WORKFLOW;
    }
    if (context instanceof MapReduceLoggingContext) {
      return ProgramType.MAPREDUCE;
    }
    if (context instanceof SparkLoggingContext) {
      return ProgramType.SPARK;
    }
    if (context instanceof UserServiceLoggingContext) {
      return ProgramType.SERVICE;
    }
    if (context instanceof WorkerLoggingContext) {
      return ProgramType.WORKER;
    }
    return null;
  }

  private static boolean tagEquals(@Nullable LoggingContext.SystemTag tag, String expected) {
    return tag != null && expected.equals(tag.getValue());
  }

  private static Decision validateMetric(byte[] payload, ProgramRunId tokenRunId)
      throws IOException {
    MetricValues metricValues = METRIC_READER.get().read(
        new BinaryDecoder(new ByteArrayInputStream(payload)), METRIC_SCHEMA);
    Map<String, String> tags = metricValues == null ? null : metricValues.getTags();
    if (tags == null) {
      return Decision.DROP_AND_COUNT;
    }
    if (!tags.containsKey(Constants.Metrics.Tag.APP)
        && !tags.containsKey(Constants.Metrics.Tag.RUN_ID)
        && !tags.containsKey(Constants.Metrics.Tag.WORKFLOW_RUN_ID)) {
      // Cluster-service metrics carry neither app nor run tags; drop without counting.
      return Decision.DROP_SILENTLY;
    }
    if (tags.containsKey(Constants.Metrics.Tag.COMPONENT)) {
      return Decision.DROP_AND_COUNT;
    }
    boolean runMatches;
    if (tags.containsKey(Constants.Metrics.Tag.WORKFLOW_RUN_ID)) {
      runMatches = tokenRunId.getType() == ProgramType.WORKFLOW
          && tokenRunId.getRun().equals(tags.get(Constants.Metrics.Tag.WORKFLOW_RUN_ID))
          && (!tags.containsKey(Constants.Metrics.Tag.RUN_ID)
              || tags.get(Constants.Metrics.Tag.RUN_ID) != null);
    } else {
      runMatches = tokenRunId.getRun().equals(tags.get(Constants.Metrics.Tag.RUN_ID));
    }
    boolean matches = tokenRunId.getNamespace().equals(tags.get(Constants.Metrics.Tag.NAMESPACE))
        && tokenRunId.getApplication().equals(tags.get(Constants.Metrics.Tag.APP))
        && runMatches;
    return matches ? Decision.ACCEPT : Decision.DROP_AND_COUNT;
  }

  private static Decision validateMetadata(byte[] payload, ProgramRunId tokenRunId) {
    MetadataMessage message = METADATA_GSON.fromJson(
        new String(payload, StandardCharsets.UTF_8), MetadataMessage.class);
    if (message == null || message.getType() == null
        || message.getRawPayload() == null || message.getRawPayload().isJsonNull()) {
      return Decision.DROP_AND_COUNT;
    }
    EntityId emitter = message.getEntityId();
    if (!isAllowedEmitter(emitter, tokenRunId)) {
      return Decision.DROP_AND_COUNT;
    }
    switch (message.getType()) {
      case WORKFLOW_TOKEN:
        if (tokenRunId.getType() != ProgramType.WORKFLOW || !tokenRunId.equals(emitter)) {
          return Decision.DROP_AND_COUNT;
        }
        return message.getPayload(METADATA_GSON, BasicWorkflowToken.class) != null
            ? Decision.ACCEPT : Decision.DROP_AND_COUNT;
      case WORKFLOW_STATE:
        if (tokenRunId.getType() != ProgramType.WORKFLOW || !tokenRunId.equals(emitter)) {
          return Decision.DROP_AND_COUNT;
        }
        WorkflowNodeStateDetail state =
            message.getPayload(METADATA_GSON, WorkflowNodeStateDetail.class);
        return state != null && !Strings.isNullOrEmpty(state.getNodeId())
            ? Decision.ACCEPT : Decision.DROP_AND_COUNT;
      case LINEAGE: {
        DataAccessLineage lineage = message.getPayload(METADATA_GSON, DataAccessLineage.class);
        return lineage != null && lineage.getAccessType() != null
            && isDatasetInNamespace(lineage.getDatasetId(), tokenRunId.getNamespace())
            ? Decision.ACCEPT : Decision.DROP_AND_COUNT;
      }
      case USAGE: {
        DatasetUsage usage = message.getPayload(METADATA_GSON, DatasetUsage.class);
        DatasetId datasetId = usage == null ? null : usage.getDatasetId();
        return isDatasetInNamespace(datasetId, tokenRunId.getNamespace())
            ? Decision.ACCEPT : Decision.DROP_AND_COUNT;
      }
      case FIELD_LINEAGE:
        return validateFieldLineage(message, tokenRunId)
            ? Decision.ACCEPT : Decision.DROP_AND_COUNT;
      case METADATA_OPERATION: {
        MetadataOperation operation = message.getPayload(METADATA_GSON, MetadataOperation.class);
        return isAllowedMetadataOperation(operation, tokenRunId)
            ? Decision.ACCEPT : Decision.DROP_AND_COUNT;
      }
      default:
        return Decision.DROP_AND_COUNT;
    }
  }

  private static boolean validateFieldLineage(MetadataMessage message, ProgramRunId tokenRunId) {
    JsonElement rawPayload = message.getRawPayload();
    if (!rawPayload.isJsonObject()) {
      return false;
    }
    JsonObject payloadObj = rawPayload.getAsJsonObject();
    if (!hasNonNullMember(payloadObj, "destinationFields")
        || !hasNonNullMember(payloadObj, "incomingSummary")
        || !hasNonNullMember(payloadObj, "outgoingSummary")) {
      return false;
    }
    // EndpointFieldDeserializer caches per message and is not thread-safe.
    Gson fieldLineageGson = new GsonBuilder()
        .registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())
        .registerTypeAdapter(MetadataOperation.class, new MetadataOperationTypeAdapter())
        .registerTypeAdapter(Operation.class, new OperationTypeAdapter())
        .registerTypeAdapter(EndPointField.class, new EndpointFieldDeserializer())
        .create();
    FieldLineageInfo info = message.getPayload(fieldLineageGson, FieldLineageInfo.class);
    if (info == null || info.getOperations() == null || info.getOperations().isEmpty()) {
      return false;
    }
    String expectedNamespace = tokenRunId.getNamespace();
    for (Operation op : info.getOperations()) {
      if (op == null || op.getType() == null) {
        return false;
      }
      if (op.getType() == OperationType.READ) {
        if (!(op instanceof ReadOperation)
            || !isValidEndPoint(((ReadOperation) op).getSource(), expectedNamespace)) {
          return false;
        }
      } else if (op.getType() == OperationType.WRITE) {
        if (!(op instanceof WriteOperation)
            || !isValidEndPoint(((WriteOperation) op).getDestination(), expectedNamespace)) {
          return false;
        }
      }
    }
    // Check for cycles iteratively before calling any lazy FieldLineageInfo getter or constructor.
    FieldLineageInfo.getTopologicallySortedOperations(info.getOperations());
    FieldLineageInfo recomputed = new FieldLineageInfo(info.getOperations());
    return info.getChecksum() == recomputed.getChecksum()
        && Objects.equals(info.getDestinationFields(), recomputed.getDestinationFields())
        && Objects.equals(info.getIncomingSummary(), recomputed.getIncomingSummary())
        && Objects.equals(info.getOutgoingSummary(), recomputed.getOutgoingSummary());
  }

  private static boolean hasNonNullMember(JsonObject obj, String memberName) {
    JsonElement member = obj.get(memberName);
    return member != null && !member.isJsonNull();
  }

  private static boolean isValidEndPoint(@Nullable EndPoint endPoint, String expectedNamespace) {
    return endPoint != null
        && expectedNamespace.equals(endPoint.getNamespace())
        && !Strings.isNullOrEmpty(endPoint.getName());
  }

  private static boolean isAllowedMetadataOperation(@Nullable MetadataOperation operation,
      ProgramRunId tokenRunId) {
    if (operation == null || operation.getType() == null) {
      return false;
    }
    MetadataScope scope;
    switch (operation.getType()) {
      case PUT: {
        MetadataOperation.Put put = (MetadataOperation.Put) operation;
        if (hasNullMapEntry(put.getProperties()) || hasNullElement(put.getTags())) {
          return false;
        }
        scope = put.getScope();
        break;
      }
      case DELETE: {
        MetadataOperation.Delete delete = (MetadataOperation.Delete) operation;
        if (hasNullElement(delete.getProperties()) || hasNullElement(delete.getTags())) {
          return false;
        }
        scope = delete.getScope();
        break;
      }
      case DELETE_ALL:
        scope = ((MetadataOperation.DeleteAll) operation).getScope();
        break;
      case DELETE_ALL_PROPERTIES:
        scope = ((MetadataOperation.DeleteAllProperties) operation).getScope();
        break;
      case DELETE_ALL_TAGS:
        scope = ((MetadataOperation.DeleteAllTags) operation).getScope();
        break;
      default:
        return false;
    }
    if (scope != MetadataScope.USER) {
      return false;
    }
    return isMetadataEntityInNamespace(operation.getEntity(), tokenRunId.getNamespace());
  }

  private static boolean hasNullMapEntry(@Nullable Map<String, String> map) {
    if (map == null) {
      return false;
    }
    for (Map.Entry<String, String> entry : map.entrySet()) {
      if (entry.getKey() == null || entry.getValue() == null) {
        return true;
      }
    }
    return false;
  }

  private static boolean hasNullElement(@Nullable Set<String> set) {
    if (set == null) {
      return false;
    }
    for (String element : set) {
      if (element == null) {
        return true;
      }
    }
    return false;
  }

  private static Decision validateAudit(byte[] payload, ProgramRunId tokenRunId) {
    AuditMessage message = AUDIT_GSON.fromJson(
        new String(payload, StandardCharsets.UTF_8), AuditMessage.class);
    if (message == null || message.getType() != AuditType.ACCESS
        || !(message.getPayload() instanceof AccessPayload)) {
      return Decision.DROP_AND_COUNT;
    }
    AccessPayload accessPayload = (AccessPayload) message.getPayload();
    if (!isAllowedEmitter(accessPayload.getAccessor(), tokenRunId)) {
      return Decision.DROP_AND_COUNT;
    }
    return isMetadataEntityInNamespace(message.getEntity(), tokenRunId.getNamespace())
        ? Decision.ACCEPT : Decision.DROP_AND_COUNT;
  }

  private static Decision validateDataEvent(byte[] payload, ProgramRunId tokenRunId) {
    Notification notification = PLAIN_GSON.fromJson(
        new String(payload, StandardCharsets.UTF_8), Notification.class);
    if (notification == null || notification.getNotificationType() != Notification.Type.PARTITION
        || notification.getProperties() == null) {
      return Decision.DROP_AND_COUNT;
    }
    String datasetIdString = notification.getProperties().get(Notification.DATASET_ID);
    if (Strings.isNullOrEmpty(datasetIdString)) {
      return Decision.DROP_AND_COUNT;
    }
    String rawNumPartitions = notification.getProperties().get(Notification.NUM_PARTITIONS);
    if (rawNumPartitions == null || Integer.parseInt(rawNumPartitions) < 0) {
      return Decision.DROP_AND_COUNT;
    }
    DatasetId datasetId = DatasetId.fromString(datasetIdString);
    return isDatasetInNamespace(datasetId, tokenRunId.getNamespace())
        ? Decision.ACCEPT : Decision.DROP_AND_COUNT;
  }

  private static boolean isDatasetInNamespace(@Nullable DatasetId datasetId,
      String expectedNamespace) {
    return RuntimeMonitors.isWellFormed(datasetId)
        && expectedNamespace.equals(datasetId.getNamespace());
  }

  private static boolean isAllowedEmitter(@Nullable EntityId id, ProgramRunId tokenRunId) {
    if (!RuntimeMonitors.isWellFormed(id)) {
      return false;
    }
    if (id.equals(tokenRunId) || id.equals(tokenRunId.getParent())) {
      return true;
    }
    if (tokenRunId.getType() != ProgramType.WORKFLOW) {
      return false;
    }
    ProgramId programId;
    if (id instanceof ProgramId) {
      programId = (ProgramId) id;
    } else if (id instanceof ProgramRunId) {
      programId = ((ProgramRunId) id).getParent();
    } else {
      return false;
    }
    ProgramType type = programId.getType();
    return (type == ProgramType.SPARK || type == ProgramType.MAPREDUCE)
        && programId.getParent().equals(tokenRunId.getParent().getParent());
  }

  private static boolean isMetadataEntityInNamespace(@Nullable MetadataEntity entity,
      String expectedNamespace) {
    if (!isWellFormedMetadataEntity(entity)) {
      return false;
    }
    MetadataEntity.KeyValue first = entity.iterator().next();
    if (!MetadataEntity.NAMESPACE.equals(first.getKey())
        || !expectedNamespace.equals(first.getValue())) {
      return false;
    }
    EntityId selfOrParent = EntityId.getSelfOrParentEntityId(entity);
    if (!(selfOrParent instanceof NamespacedEntityId)
        || !expectedNamespace.equals(((NamespacedEntityId) selfOrParent).getNamespace())) {
      return false;
    }
    // PluginId.fromIdParts swaps type and plugin on round-trip; getSelfOrParentEntityId already
    // validated its parts through PluginId's constructor.
    return (selfOrParent instanceof PluginId) || RuntimeMonitors.isWellFormed(selfOrParent);
  }

  private static boolean isWellFormedMetadataEntity(@Nullable MetadataEntity entity) {
    if (entity == null) {
      return false;
    }
    String type = entity.getType();
    if (Strings.isNullOrEmpty(type)) {
      return false;
    }
    MetadataEntity.Builder builder = MetadataEntity.builder();
    boolean typeMatched = false;
    for (MetadataEntity.KeyValue kv : entity) {
      String key = kv.getKey();
      String value = kv.getValue();
      if (Strings.isNullOrEmpty(key) || value == null) {
        return false;
      }
      if (key.equals(type)) {
        builder.appendAsType(key, value);
        typeMatched = true;
      } else {
        builder.append(key, value);
      }
    }
    if (!typeMatched) {
      return false;
    }
    MetadataEntity rebuilt = builder.build();
    return entity.equals(rebuilt) && rebuilt.equals(entity);
  }

  private static boolean matchesTopicOrPartition(String topic, @Nullable String prefix) {
    if (Strings.isNullOrEmpty(prefix)) {
      return false;
    }
    if (topic.equals(prefix)) {
      return true;
    }
    if (!topic.startsWith(prefix)) {
      return false;
    }
    for (int i = prefix.length(); i < topic.length(); i++) {
      char c = topic.charAt(i);
      if (c < '0' || c > '9') {
        return false;
      }
    }
    return true;
  }

  private static Schema generateMetricSchema() {
    try {
      return new ReflectionSchemaGenerator().generate(MetricValues.class);
    } catch (UnsupportedTypeException e) {
      throw new IllegalStateException("Failed to generate schema for MetricValues", e);
    }
  }
}
