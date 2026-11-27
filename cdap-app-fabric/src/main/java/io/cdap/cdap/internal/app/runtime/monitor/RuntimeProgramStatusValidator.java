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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.api.service.ServiceUnavailableException;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.utils.TimeProvider;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.codec.BasicThrowableCodec;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramRunId;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates program status and heartbeat notifications relayed by remote program runs authenticated
 * with a {@code RUNTIME} token.
 */
@Singleton
final class RuntimeProgramStatusValidator {

  private static final Logger LOG = LoggerFactory.getLogger(RuntimeProgramStatusValidator.class);
  private static final Gson GSON = new GsonBuilder()
      .registerTypeAdapter(BasicThrowable.class, new BasicThrowableCodec())
      .create();

  private static final Set<ProgramRunStatus> ALLOWED_REMOTE_STATUSES =
      Collections.unmodifiableSet(EnumSet.of(
          ProgramRunStatus.RUNNING,
          ProgramRunStatus.COMPLETED,
          ProgramRunStatus.FAILED,
          ProgramRunStatus.KILLED));

  static final long UNKNOWN_WAIT_MILLIS = TimeUnit.SECONDS.toMillis(60);
  static final long MEMORY_EXPIRY_MILLIS = TimeUnit.MINUTES.toMillis(10);
  static final int MAX_RECHECKS_PER_REQUEST = 4;
  static final int MAX_ERROR_CAUSE_DEPTH = 64;
  static final int DEFAULT_MAX_ADMITTED_INNER_RUNS = 10_000;
  static final int DEFAULT_MAX_UNKNOWN_RUNS = 10_000;
  static final int DEFAULT_MAX_REPORT_ONLY_NOTES = 10_000;

  private final RuntimeRequestValidator requestValidator;
  private final RuntimeValidationReporter validationReporter;
  private final TimeProvider timeProvider;
  private final int maxReportOnlyNotes;

  /** Inner run id -> workflow run id whose STARTING notification was admitted by this server. */
  private final Cache<ProgramRunId, ProgramRunId> admittedInnerRuns;

  /** (inner run, workflow run) -> timestamp (ms) when the run was first seen as unknown. */
  private final Cache<RunPair, Long> unknownFirstSeen;

  /**
   * Deferred notes kept in report-only mode until re-checked on a later status request.
   * All access to {@code reportOnlyNotes} and {@code inFlightRechecks} is synchronized on
   * {@code reportOnlyNotes}.
   */
  private final Map<RunPair, ReportOnlyNote> reportOnlyNotes;
  private int inFlightRechecks;

  @Inject
  RuntimeProgramStatusValidator(RuntimeRequestValidator requestValidator,
      RuntimeValidationReporter validationReporter) {
    this(requestValidator, validationReporter, TimeProvider.SYSTEM_TIME);
  }

  RuntimeProgramStatusValidator(RuntimeRequestValidator requestValidator,
      RuntimeValidationReporter validationReporter, TimeProvider timeProvider) {
    this(requestValidator, validationReporter, timeProvider,
        DEFAULT_MAX_ADMITTED_INNER_RUNS, DEFAULT_MAX_UNKNOWN_RUNS, DEFAULT_MAX_REPORT_ONLY_NOTES);
  }

  @VisibleForTesting
  RuntimeProgramStatusValidator(RuntimeRequestValidator requestValidator,
      RuntimeValidationReporter validationReporter, TimeProvider timeProvider,
      int maxAdmittedInnerRuns, int maxUnknownRuns, int maxReportOnlyNotes) {
    this.requestValidator = requestValidator;
    this.validationReporter = validationReporter;
    this.timeProvider = timeProvider;
    this.maxReportOnlyNotes = maxReportOnlyNotes;

    final long baseMillis = timeProvider.currentTimeMillis();
    Ticker ticker = new Ticker() {
      @Override
      public long read() {
        return TimeUnit.MILLISECONDS.toNanos(timeProvider.currentTimeMillis() - baseMillis);
      }
    };
    this.admittedInnerRuns = CacheBuilder.newBuilder()
        .maximumSize(maxAdmittedInnerRuns)
        .expireAfterWrite(MEMORY_EXPIRY_MILLIS, TimeUnit.MILLISECONDS)
        .ticker(ticker)
        .build();
    this.unknownFirstSeen = CacheBuilder.newBuilder()
        .maximumSize(maxUnknownRuns)
        .expireAfterWrite(MEMORY_EXPIRY_MILLIS, TimeUnit.MILLISECONDS)
        .ticker(ticker)
        .build();
    this.reportOnlyNotes = new LinkedHashMap<>();
  }

  /**
   * Creates a per-request validator for a program-status topic post from an authenticated
   * {@code RUNTIME} token run.
   *
   * @param tokenRunId program run identity from the validated {@code RUNTIME} token
   * @param topic target system TMS topic name
   * @return a stateful per-request validator that deduplicates lookups within the request
   */
  RequestValidator newRequestValidator(ProgramRunId tokenRunId, String topic) {
    return new RequestValidator(tokenRunId, topic);
  }

  @VisibleForTesting
  long getAdmittedInnerRunsSize() {
    admittedInnerRuns.cleanUp();
    return admittedInnerRuns.size();
  }

  @VisibleForTesting
  long getUnknownFirstSeenSize() {
    unknownFirstSeen.cleanUp();
    return unknownFirstSeen.size();
  }

  @VisibleForTesting
  int getReportOnlyNotesSize() {
    synchronized (reportOnlyNotes) {
      return reportOnlyNotes.size();
    }
  }

  private long getOrRecordUnknownFirstSeen(RunPair key, long now) {
    Long existing = unknownFirstSeen.getIfPresent(key);
    if (existing != null) {
      return existing;
    }
    Long previous = unknownFirstSeen.asMap().putIfAbsent(key, now);
    return previous != null ? previous : now;
  }

  /**
   * Records a report-only note for an unknown run. Returns {@code true} if the note is recorded
   * (or already existed), or {@code false} if the note capacity has been reached.
   */
  private boolean recordReportOnlyNote(RunPair key, String topic, long now) {
    synchronized (reportOnlyNotes) {
      if (reportOnlyNotes.containsKey(key)) {
        return true;
      }
      if (reportOnlyNotes.size() + inFlightRechecks >= maxReportOnlyNotes) {
        return false;
      }
      reportOnlyNotes.put(key, new ReportOnlyNote(key, topic, now));
      return true;
    }
  }

  private void removeReportOnlyNote(RunPair key) {
    synchronized (reportOnlyNotes) {
      reportOnlyNotes.remove(key);
    }
  }

  private void recheckDueNotes() {
    long now = timeProvider.currentTimeMillis();
    for (int i = 0; i < MAX_RECHECKS_PER_REQUEST; i++) {
      ReportOnlyNote note = claimOneDueNote(now);
      if (note == null) {
        return;
      }
      boolean completed = false;
      try {
        completed = recheckSingleNote(note);
      } finally {
        finishClaimedNote(note, completed);
      }
      if (!completed) {
        return;
      }
    }
  }

  @Nullable
  private ReportOnlyNote claimOneDueNote(long now) {
    synchronized (reportOnlyNotes) {
      Iterator<Map.Entry<RunPair, ReportOnlyNote>> it = reportOnlyNotes.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry<RunPair, ReportOnlyNote> entry = it.next();
        if (now - entry.getValue().firstSeenMillis >= UNKNOWN_WAIT_MILLIS) {
          it.remove();
          inFlightRechecks++;
          return entry.getValue();
        }
      }
      return null;
    }
  }

  private void finishClaimedNote(ReportOnlyNote note, boolean completed) {
    synchronized (reportOnlyNotes) {
      inFlightRechecks--;
      if (!completed) {
        ReportOnlyNote existing = reportOnlyNotes.get(note.key);
        if (existing == null || note.firstSeenMillis < existing.firstSeenMillis) {
          reportOnlyNotes.put(note.key, note);
        }
      }
    }
  }

  /**
   * Re-checks one due report-only note. Returns {@code true} if the note was decided (whether
   * counted or cleared), or {@code false} if a lookup failed so the note must be kept.
   */
  private boolean recheckSingleNote(ReportOnlyNote note) {
    try {
      ProgramRunId innerRun = note.key.innerRun;
      ProgramRunId workflowRun = note.key.workflowRun;
      Optional<ProgramRunId> topLevelRun = requestValidator.getTopLevelRun(innerRun);
      if (topLevelRun.isPresent()) {
        if (!workflowRun.equals(topLevelRun.get())) {
          validationReporter.reportMessageDrops(
              RuntimeValidationFailure.MESSAGE_STATUS, workflowRun, note.topic, 1L);
        }
        return true;
      }
      if (workflowRun.equals(admittedInnerRuns.getIfPresent(innerRun))) {
        return true;
      }
      // Look up workflowRun only when innerRun has no record and is not remembered for it; if
      // neither has a record any more (application deleted), clear the note without counting.
      Optional<ProgramRunId> workflowRecord = requestValidator.getTopLevelRun(workflowRun);
      if (!workflowRecord.isPresent()) {
        return true;
      }
      validationReporter.reportMessageDrops(
          RuntimeValidationFailure.MESSAGE_STATUS, workflowRun, note.topic, 1L);
      return true;
    } catch (Exception e) {
      LOG.debug("Failed to re-check deferred status note for run {} under {}",
          note.key.innerRun, note.key.workflowRun, e);
      return false;
    }
  }

  /**
   * Action for the HTTP body consumer to take on a single decoded program-status payload.
   */
  enum Action {
    FORWARD,
    SKIP,
    WAIT
  }

  /**
   * Per-request validator for a single program-status message post. Not thread-safe; used from the
   * Netty handler's single-threaded {@link io.cdap.http.BodyConsumer} callbacks for one request.
   */
  final class RequestValidator {

    private final ProgramRunId tokenRunId;
    private final ApplicationId tokenAppId;
    private final String topic;
    private final boolean enforcing;
    private final Map<ProgramRunId, Optional<ProgramRunId>> requestLookups;
    private final Map<ProgramRunId, ServiceUnavailableException> failedLookups;
    private long droppedCount;
    private boolean rechecked;

    private RequestValidator(ProgramRunId tokenRunId, String topic) {
      this.tokenRunId = tokenRunId;
      this.tokenAppId = tokenRunId.getParent().getParent();
      this.topic = topic;
      this.enforcing = validationReporter.isValidationEnabled();
      this.requestLookups = new HashMap<>();
      this.failedLookups = new HashMap<>();
    }

    /**
     * Validates one program-status or heartbeat payload and returns the action to take.
     */
    Action apply(@Nullable byte[] payload) {
      if (payload == null) {
        return recordDrop();
      }
      try {
        return validatePayload(payload);
      } catch (ServiceUnavailableException e) {
        LOG.debug("Transient run lookup failure while validating status payload for {}",
            tokenRunId, e);
        return enforcing ? Action.WAIT : Action.FORWARD;
      } catch (Exception | StackOverflowError e) {
        LOG.debug("Failed to decode or validate status payload for {}", tokenRunId, e);
        return recordDrop();
      }
    }

    /**
     * Flushes any per-request failure count and, in report-only mode, re-checks up to
     * {@link #MAX_RECHECKS_PER_REQUEST} due notes. Idempotent across repeated calls.
     */
    void finish() {
      if (droppedCount > 0L) {
        long count = droppedCount;
        droppedCount = 0L;
        validationReporter.reportMessageDrops(
            RuntimeValidationFailure.MESSAGE_STATUS, tokenRunId, topic, count);
      }
      if (!enforcing && !rechecked) {
        rechecked = true;
        recheckDueNotes();
      }
    }

    private Action validatePayload(byte[] payload) {
      Notification notification = GSON.fromJson(
          new String(payload, StandardCharsets.UTF_8), Notification.class);
      if (notification == null) {
        return recordDrop();
      }
      Notification.Type type = notification.getNotificationType();
      if (type != Notification.Type.PROGRAM_STATUS && type != Notification.Type.PROGRAM_HEART_BEAT) {
        return recordDrop();
      }
      Map<String, String> properties = notification.getProperties();
      if (properties == null || properties.containsKey(ProgramOptionConstants.CLUSTER_STATUS)) {
        return recordDrop();
      }

      String rawRunId = properties.get(ProgramOptionConstants.PROGRAM_RUN_ID);
      if (Strings.isNullOrEmpty(rawRunId)) {
        return recordDrop();
      }
      ProgramRunId runId = GSON.fromJson(rawRunId, ProgramRunId.class);
      if (!RuntimeMonitors.isWellFormed(runId)
          || !tokenAppId.equals(runId.getParent().getParent())) {
        return recordDrop();
      }

      if (type == Notification.Type.PROGRAM_HEART_BEAT) {
        return validateHeartbeat(properties, runId);
      }
      return validateProgramStatus(notification, properties, runId);
    }

    private Action validateHeartbeat(Map<String, String> properties, ProgramRunId runId) {
      if (properties.containsKey(ProgramOptionConstants.PROGRAM_STATUS)) {
        return recordDrop();
      }
      String rawHeartBeatTime = properties.get(ProgramOptionConstants.HEART_BEAT_TIME);
      if (rawHeartBeatTime == null) {
        return recordDrop();
      }
      Long.parseLong(rawHeartBeatTime);
      // Only a top-level run emits heartbeats; no lookup is performed.
      if (!runId.equals(tokenRunId)) {
        return recordDrop();
      }
      return Action.FORWARD;
    }

    private Action validateProgramStatus(Notification notification, Map<String, String> properties,
        ProgramRunId runId) {
      String rawStatus = properties.get(ProgramOptionConstants.PROGRAM_STATUS);
      if (rawStatus == null) {
        return recordDrop();
      }
      ProgramRunStatus status = ProgramRunStatus.valueOf(rawStatus);

      if (status == ProgramRunStatus.RUNNING) {
        String rawLogicalStartTime = properties.get(ProgramOptionConstants.LOGICAL_START_TIME);
        if (rawLogicalStartTime == null) {
          return recordDrop();
        }
        Long.parseLong(rawLogicalStartTime);
      } else if (status == ProgramRunStatus.COMPLETED
          || status == ProgramRunStatus.FAILED
          || status == ProgramRunStatus.KILLED) {
        String rawEndTime = properties.get(ProgramOptionConstants.END_TIME);
        if (rawEndTime == null) {
          return recordDrop();
        }
        Long.parseLong(rawEndTime);
        String rawError = properties.get(ProgramOptionConstants.PROGRAM_ERROR);
        if (rawError != null) {
          BasicThrowable error = GSON.fromJson(rawError, BasicThrowable.class);
          if (!isWithinErrorCauseDepth(error)) {
            return recordDrop();
          }
        }
      }

      if (runId.equals(tokenRunId)) {
        return ALLOWED_REMOTE_STATUSES.contains(status) ? Action.FORWARD : recordDrop();
      }
      if (status == ProgramRunStatus.STARTING) {
        return validateInnerStarting(notification, properties, runId);
      }
      return validateInnerOtherStatus(runId, status);
    }

    private Action validateInnerStarting(Notification notification, Map<String, String> properties,
        ProgramRunId runId) {
      if (!isWorkflowInnerCandidate(runId)) {
        return recordDrop();
      }
      String rawArtifactId = properties.get(ProgramOptionConstants.PROGRAM_ARTIFACT_ID);
      if (rawArtifactId == null) {
        return recordDrop();
      }
      ArtifactId artifactId = GSON.fromJson(rawArtifactId, ArtifactId.class);
      if (!RuntimeMonitors.isWellFormed(artifactId)
          || (!runId.getNamespace().equals(artifactId.getNamespace())
              && !NamespaceId.SYSTEM.getNamespace().equals(artifactId.getNamespace()))) {
        return recordDrop();
      }
      // Validate artifact version format; throws on malformed versions.
      artifactId.toApiArtifactId();

      ProgramOptions options = ProgramOptions.fromNotification(notification, GSON);
      Map<String, String> systemArgs = options.getArguments().asMap();
      if (!tokenRunId.getProgram().equals(systemArgs.get(ProgramOptionConstants.WORKFLOW_NAME))
          || !tokenRunId.getRun().equals(systemArgs.get(ProgramOptionConstants.WORKFLOW_RUN_ID))
          || Strings.isNullOrEmpty(systemArgs.get(ProgramOptionConstants.WORKFLOW_NODE_ID))) {
        return recordDrop();
      }
      Optional<ProfileId> profileId =
          SystemArguments.getProfileIdFromArgs(runId.getNamespaceId(), systemArgs);
      if (!profileId.isPresent() || !RuntimeMonitors.isWellFormed(profileId.get())) {
        return recordDrop();
      }

      ProgramRunId rememberedOwner = admittedInnerRuns.getIfPresent(runId);
      if (rememberedOwner != null && !tokenRunId.equals(rememberedOwner)) {
        return recordDrop();
      }
      Optional<ProgramRunId> topLevelRun = lookupTopLevelRun(runId);
      if (topLevelRun.isPresent() && !tokenRunId.equals(topLevelRun.get())) {
        return recordDrop();
      }

      ProgramRunId existing = admittedInnerRuns.asMap().putIfAbsent(runId, tokenRunId);
      if (existing != null && !tokenRunId.equals(existing)) {
        return recordDrop();
      }
      RunPair pair = new RunPair(runId, tokenRunId);
      unknownFirstSeen.invalidate(pair);
      removeReportOnlyNote(pair);
      return Action.FORWARD;
    }

    private Action validateInnerOtherStatus(ProgramRunId runId, ProgramRunStatus status) {
      if (!isWorkflowInnerCandidate(runId) || !ALLOWED_REMOTE_STATUSES.contains(status)) {
        return recordDrop();
      }

      // App-Fabric's record is authoritative once it exists; check it before server memory.
      Optional<ProgramRunId> topLevelRun = lookupTopLevelRun(runId);
      if (topLevelRun.isPresent()) {
        return tokenRunId.equals(topLevelRun.get()) ? Action.FORWARD : recordDrop();
      }
      if (tokenRunId.equals(admittedInnerRuns.getIfPresent(runId))) {
        return Action.FORWARD;
      }

      RunPair pair = new RunPair(runId, tokenRunId);
      long now = timeProvider.currentTimeMillis();
      if (enforcing) {
        long firstSeen = getOrRecordUnknownFirstSeen(pair, now);
        if (now - firstSeen >= UNKNOWN_WAIT_MILLIS) {
          return recordDrop();
        }
        return Action.WAIT;
      }
      if (recordReportOnlyNote(pair, topic, now)) {
        return Action.FORWARD;
      }
      return recordDrop();
    }

    private boolean isWorkflowInnerCandidate(ProgramRunId runId) {
      if (tokenRunId.getType() != ProgramType.WORKFLOW) {
        return false;
      }
      ProgramType type = runId.getType();
      return type == ProgramType.SPARK || type == ProgramType.MAPREDUCE;
    }

    private boolean isWithinErrorCauseDepth(@Nullable BasicThrowable error) {
      int depth = 0;
      for (BasicThrowable current = error; current != null; current = current.getCause()) {
        if (++depth > MAX_ERROR_CAUSE_DEPTH) {
          return false;
        }
      }
      return true;
    }

    private Optional<ProgramRunId> lookupTopLevelRun(ProgramRunId runId) {
      Optional<ProgramRunId> cached = requestLookups.get(runId);
      if (cached != null) {
        return cached;
      }
      ServiceUnavailableException failed = failedLookups.get(runId);
      if (failed != null) {
        throw failed;
      }
      try {
        Optional<ProgramRunId> result = requestValidator.getTopLevelRun(runId);
        requestLookups.put(runId, result);
        return result;
      } catch (ServiceUnavailableException e) {
        failedLookups.put(runId, e);
        throw e;
      }
    }

    private Action recordDrop() {
      droppedCount++;
      return enforcing ? Action.SKIP : Action.FORWARD;
    }
  }

  private static final class RunPair {

    private final ProgramRunId innerRun;
    private final ProgramRunId workflowRun;

    private RunPair(ProgramRunId innerRun, ProgramRunId workflowRun) {
      this.innerRun = innerRun;
      this.workflowRun = workflowRun;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof RunPair)) {
        return false;
      }
      RunPair other = (RunPair) o;
      return innerRun.equals(other.innerRun) && workflowRun.equals(other.workflowRun);
    }

    @Override
    public int hashCode() {
      return Objects.hash(innerRun, workflowRun);
    }
  }

  private static final class ReportOnlyNote {

    private final RunPair key;
    private final String topic;
    private final long firstSeenMillis;

    private ReportOnlyNote(RunPair key, String topic, long firstSeenMillis) {
      this.key = key;
      this.topic = topic;
      this.firstSeenMillis = firstSeenMillis;
    }
  }
}
