/*
 * Copyright © 2020 Cask Data, Inc.
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
import com.google.common.base.MoreObjects;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.google.inject.Inject;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.api.service.ServiceUnavailableException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.GoneException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.io.Codec;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.logging.gateway.handlers.ProgramRunRecordFetcher;
import io.cdap.cdap.proto.ProgramRunCluster;
import io.cdap.cdap.proto.ProgramRunClusterStatus;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.auth.AccessToken;
import io.cdap.cdap.security.auth.InvalidTokenException;
import io.cdap.cdap.security.auth.TokenManager;
import io.cdap.cdap.security.auth.UserIdentity;
import io.cdap.cdap.security.authorization.AuthorizationUtil;
import io.cdap.cdap.security.impersonation.SecurityUtil;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authentication.UnauthenticatedException;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.netty.handler.codec.http.HttpRequest;
import java.io.IOException;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link RuntimeRequestValidator} implementation that reads from the runtime table directly.
 */
public final class DirectRuntimeRequestValidator implements RuntimeRequestValidator {

  private static final Logger LOG = LoggerFactory.getLogger(DirectRuntimeRequestValidator.class);
  private static final Gson GSON = new Gson();

  private final boolean internalAuthEnabled;
  private final TransactionRunner txRunner;
  private final ProgramRunRecordFetcher runRecordFetcher;
  private final LoadingCache<ProgramRunId, RunLookupResult> programRunsCache;
  private final AccessEnforcer accessEnforcer;
  private final AuthenticationContext authenticationContext;
  private final TokenManager tokenManager;
  private final Codec<AccessToken> accessTokenCodec;
  private final RuntimeValidationReporter validationReporter;

  @Inject
  DirectRuntimeRequestValidator(CConfiguration cConf, TransactionRunner txRunner,
      ProgramRunRecordFetcher runRecordFetcher,
      AccessEnforcer accessEnforcer, AuthenticationContext authenticationContext,
      TokenManager tokenManager, Codec<AccessToken> accessTokenCodec,
      RuntimeValidationReporter validationReporter) {
    this.internalAuthEnabled = SecurityUtil.isInternalAuthEnabled(cConf);
    this.txRunner = txRunner;
    this.runRecordFetcher = runRecordFetcher;
    this.accessEnforcer = accessEnforcer;
    this.authenticationContext = authenticationContext;
    this.tokenManager = tokenManager;
    this.accessTokenCodec = accessTokenCodec;
    this.validationReporter = validationReporter;

    // Configure the cache with expiry the poll time.
    // This helps reduce the actual lookup for a burst of requests within one poll interval,
    // but not to keep it too long so that data becomes stale.
    // LoadingCache single-flights concurrent cold lookups so duplicate local inserts are avoided;
    // results that must not be cached (NOT_LAUNCHED) are removed by identity right after load.
    long pollTimeMillis = cConf.getLong(Constants.RuntimeMonitor.POLL_TIME_MS);
    this.programRunsCache = CacheBuilder.newBuilder()
        .expireAfterWrite(pollTimeMillis, TimeUnit.MILLISECONDS)
        .build(new CacheLoader<ProgramRunId, RunLookupResult>() {
          @Override
          public RunLookupResult load(ProgramRunId programRunId) throws Exception {
            return loadRun(programRunId);
          }
        });
  }

  @Override
  public ValidatedRuntimeRequest validate(ProgramRunId programRunId, HttpRequest request)
      throws BadRequestException, AccessException, GoneException {
    AccessToken runtimeToken = internalAuthEnabled ? extractRuntimeToken(request) : null;
    if (runtimeToken == null) {
      return validateLegacyRequest(programRunId, request);
    }
    return validateRuntimeTokenRequest(programRunId, request, runtimeToken);
  }

  @Override
  public Optional<ProgramRunId> getTopLevelRun(ProgramRunId programRunId) {
    if (!RuntimeMonitors.isWellFormed(programRunId)) {
      return Optional.empty();
    }
    RunLookupResult cached = programRunsCache.getIfPresent(programRunId);
    if (cached != null
        && cached.getState() != LookupState.NOT_FOUND
        && cached.getState() != LookupState.NOT_LAUNCHED) {
      return Optional.ofNullable(cached.getTopLevelRun());
    }
    // Load directly so getTopLevelRun neither reuses a cached NOT_FOUND answer nor leaves a
    // NOT_FOUND or NOT_LAUNCHED entry in the shared cache.
    RunLookupResult loaded;
    try {
      loaded = loadRun(programRunId);
    } catch (Exception e) {
      throw new ServiceUnavailableException(Constants.Service.RUNTIME, e);
    }
    if (loaded.getState() == LookupState.NOT_FOUND) {
      return Optional.empty();
    }
    if (loaded.getState() == LookupState.NOT_LAUNCHED) {
      if (cached != null) {
        programRunsCache.asMap().remove(programRunId, cached);
      }
      return Optional.ofNullable(loaded.getTopLevelRun());
    }
    programRunsCache.put(programRunId, loaded);
    return Optional.ofNullable(loaded.getTopLevelRun());
  }

  private ValidatedRuntimeRequest validateLegacyRequest(ProgramRunId urlRunId, HttpRequest request)
      throws BadRequestException, AccessException, GoneException {
    // Enforce access before any local or remote run-record lookup so unauthorized calls fail fast.
    accessEnforcer.enforce(urlRunId, authenticationContext.getPrincipal(), StandardPermission.GET);
    RunLookupResult urlRunResult = lookupForRequest(urlRunId);
    ProgramRunInfo runInfo = requireActiveForRequest(urlRunId, urlRunResult);

    if (!internalAuthEnabled) {
      return ValidatedRuntimeRequest.legacy(runInfo);
    }

    ProgramRunId topRun = urlRunResult.getTopLevelRun();
    boolean hasScopesArg = urlRunId.equals(topRun)
        ? urlRunResult.hasScopesArg() : lookupForRequest(topRun).hasScopesArg();
    if (hasScopesArg) {
      if (validationReporter.reportFailure(RuntimeValidationFailure.LEGACY_TOKEN, urlRunId,
          Constants.Service.RUNTIME, request.method().name(), stripQuery(request.uri()))) {
        throw new RuntimeTokenRefusedException(RuntimeValidationFailure.LEGACY_TOKEN,
            "Program run " + urlRunId + " requires a runtime token");
      }
    }
    return ValidatedRuntimeRequest.legacy(runInfo);
  }

  private ValidatedRuntimeRequest validateRuntimeTokenRequest(ProgramRunId urlRunId,
      HttpRequest request, AccessToken runtimeToken)
      throws BadRequestException, AccessException, GoneException {
    try {
      tokenManager.validateSecret(runtimeToken);
    } catch (InvalidTokenException e) {
      throw new UnauthenticatedException("Invalid runtime token", e);
    }

    ProgramRunId tokenRunId = parseTokenRunId(runtimeToken.getIdentifier().getUsername());
    if (tokenRunId == null) {
      throw new UnauthenticatedException("Runtime token identity is not a valid program run id");
    }

    RunLookupResult tokenRunResult = lookupForRequest(tokenRunId);
    ProgramRunInfo tokenRunInfo = requireActiveForRequest(tokenRunId, tokenRunResult);

    if (!RuntimeMonitors.isWellFormed(urlRunId)) {
      throw new BadRequestException("Program run " + urlRunId + " is not valid");
    }

    ProgramRunInfo runInfo;
    if (isSameRunExceptVersion(urlRunId, tokenRunId)) {
      // Do not look up urlRunId so -SNAPSHOT URLs do not cause an extra lookup or local record.
      runInfo = tokenRunInfo;
    } else {
      ProgramType urlType = urlRunId.getType();
      boolean canBeInnerRun = tokenRunId.getType() == ProgramType.WORKFLOW
          && (urlType == ProgramType.SPARK || urlType == ProgramType.MAPREDUCE)
          && urlRunId.getParent().getParent().equals(tokenRunId.getParent().getParent());
      if (!canBeInnerRun) {
        reportUrlMismatch(urlRunId, tokenRunId, request);
      }
      RunLookupResult urlRunResult = lookupForRequest(urlRunId);
      runInfo = requireActiveForRequest(urlRunId, urlRunResult);
      if (canBeInnerRun && !tokenRunId.equals(urlRunResult.getTopLevelRun())) {
        reportUrlMismatch(urlRunId, tokenRunId, request);
      }
    }

    if (tokenRunResult.getRuntimeScopes() == null) {
      if (validationReporter.reportFailure(RuntimeValidationFailure.MISSING_SCOPES, tokenRunId,
          Constants.Service.RUNTIME, request.method().name(), stripQuery(request.uri()))) {
        throw new RuntimeTokenRefusedException(RuntimeValidationFailure.MISSING_SCOPES,
            "Program run " + tokenRunId + " has no runtime scopes");
      }
    }
    return ValidatedRuntimeRequest.runtimeToken(
        runInfo, tokenRunId, tokenRunResult.getRuntimeScopes());
  }

  private void reportUrlMismatch(ProgramRunId urlRunId, ProgramRunId tokenRunId,
      HttpRequest request) throws RuntimeTokenRefusedException {
    if (validationReporter.reportFailure(RuntimeValidationFailure.URL_MISMATCH, tokenRunId,
        Constants.Service.RUNTIME, request.method().name(), stripQuery(request.uri()))) {
      throw new RuntimeTokenRefusedException(RuntimeValidationFailure.URL_MISMATCH,
          "Program run " + urlRunId + " does not belong to token run " + tokenRunId);
    }
  }

  /**
   * Decodes the {@link Constants.Security.Headers#RUNTIME_TOKEN} header in memory and returns the
   * {@link AccessToken} if it is an {@link Credential.CredentialType#INTERNAL} credential carrying
   * a {@link UserIdentity.IdentifierType#RUNTIME} identity; otherwise returns {@code null} (legacy).
   */
  @Nullable
  private AccessToken extractRuntimeToken(HttpRequest request) {
    String authHeader = request.headers().get(Constants.Security.Headers.RUNTIME_TOKEN);
    if (authHeader == null) {
      return null;
    }
    String trimmed = authHeader.trim();
    int idx = trimmed.indexOf(' ');
    if (idx < 0) {
      return null;
    }
    Credential.CredentialType credentialType;
    try {
      credentialType = Credential.CredentialType.fromQualifiedName(trimmed.substring(0, idx));
    } catch (IllegalArgumentException e) {
      return null;
    }
    if (credentialType != Credential.CredentialType.INTERNAL) {
      return null;
    }
    String credentialValue = trimmed.substring(idx + 1).trim();
    AccessToken token;
    try {
      token = accessTokenCodec.decode(Base64.getDecoder().decode(credentialValue));
    } catch (Exception e) {
      return null;
    }
    if (token == null || token.getIdentifier() == null
        || token.getIdentifier().getIdentifierType() != UserIdentity.IdentifierType.RUNTIME) {
      return null;
    }
    return token;
  }

  @Nullable
  private static ProgramRunId parseTokenRunId(@Nullable String username) {
    if (username == null) {
      return null;
    }
    try {
      ProgramRunId runId = GSON.fromJson(username, ProgramRunId.class);
      return RuntimeMonitors.isWellFormed(runId) ? runId : null;
    } catch (Exception e) {
      return null;
    }
  }

  @VisibleForTesting
  static boolean isSameRunExceptVersion(ProgramRunId first, ProgramRunId second) {
    return first.getParent().isSameProgramExceptVersion(second.getParent())
        && first.getRun().equals(second.getRun());
  }

  private RunLookupResult lookupForRequest(ProgramRunId programRunId) throws BadRequestException {
    if (!RuntimeMonitors.isWellFormed(programRunId)) {
      throw new BadRequestException("Program run " + programRunId + " is not valid");
    }
    RunLookupResult result;
    try {
      result = programRunsCache.get(programRunId);
    } catch (Exception e) {
      throw new ServiceUnavailableException(Constants.Service.RUNTIME, e);
    }
    if (result.getState() == LookupState.NOT_LAUNCHED) {
      // Remove by object identity so a concurrently loaded cacheable entry is never evicted.
      programRunsCache.asMap().remove(programRunId, result);
    }
    return result;
  }

  private static ProgramRunInfo requireActiveForRequest(ProgramRunId programRunId,
      RunLookupResult result) throws BadRequestException, GoneException {
    switch (result.getState()) {
      case ACTIVE:
        return result.getProgramRunInfo();
      case FINISHED_LOCAL:
        ProgramRunInfo finishedInfo = result.getProgramRunInfo();
        throw new GoneException(String.format("Program run %s status is %s",
            programRunId, finishedInfo.getProgramRunStatus()));
      case NOT_FOUND:
      case NOT_LAUNCHED:
      case FINISHED_FETCHED:
      default:
        throw new BadRequestException("Program run " + programRunId + " is not valid");
    }
  }

  private RunLookupResult loadRun(ProgramRunId programRunId) throws Exception {
    return AuthorizationUtil.callAsSystem(() -> {
      try {
        RunRecordDetail runRecord = TransactionRunners.run(txRunner, context -> {
          return AppMetadataStore.create(context).getRun(programRunId);
        }, IOException.class);
        if (runRecord != null) {
          return classifyRunRecord(runRecord, false);
        }
        // If it is not found in the local store, which should be very rare, try to fetch the run record remotely.
        LOG.info("Remotely fetching program run details for {}", programRunId);
        runRecord = runRecordFetcher.getRunRecordMeta(programRunId);
        RunLookupResult result = classifyRunRecord(runRecord, true);
        if (result.getState() == LookupState.ACTIVE) {
          insertRunRecord(programRunId, runRecord);
        }
        return result;
      } catch (NotFoundException e) {
        return RunLookupResult.notFound();
      }
    });
  }

  private RunLookupResult classifyRunRecord(RunRecordDetail runRecord, boolean fetched) {
    ProgramRunId topLevelRun = extractTopLevelRun(runRecord);
    String rawScopes = runRecord.getSystemArgs().get(ProgramOptionConstants.RUNTIME_SCOPES);
    // Track key presence separately from parsed scopes so unparseable scopes still reject
    // legacy tokens.
    boolean hasScopesArg = rawScopes != null;
    RuntimeScopes runtimeScopes = parseRuntimeScopes(runRecord.getProgramRunId(), rawScopes);

    ProgramRunStatus status = runRecord.getStatus();
    if (status.isEndState()) {
      LookupState state = fetched ? LookupState.FINISHED_FETCHED : LookupState.FINISHED_LOCAL;
      return new RunLookupResult(state, buildRunInfo(runRecord), topLevelRun, hasScopesArg,
          runtimeScopes);
    }

    ProgramRunCluster cluster = runRecord.getCluster();
    if (status == ProgramRunStatus.PENDING
        || (cluster != null && cluster.getStatus() == ProgramRunClusterStatus.PROVISIONING)) {
      return new RunLookupResult(LookupState.NOT_LAUNCHED, null, topLevelRun, hasScopesArg,
          runtimeScopes);
    }

    return new RunLookupResult(LookupState.ACTIVE, buildRunInfo(runRecord), topLevelRun,
        hasScopesArg, runtimeScopes);
  }

  private static ProgramRunId extractTopLevelRun(RunRecordDetail runRecord) {
    ProgramRunId runId = runRecord.getProgramRunId();
    Map<String, String> systemArgs = runRecord.getSystemArgs();
    String workflowName = systemArgs.get(ProgramOptionConstants.WORKFLOW_NAME);
    String workflowRun = systemArgs.get(ProgramOptionConstants.WORKFLOW_RUN_ID);
    if (workflowName == null || workflowRun == null) {
      return runId;
    }
    try {
      ApplicationId appId = runId.getParent().getParent();
      ProgramRunId parentWorkflowRunId =
          new ProgramRunId(appId, ProgramType.WORKFLOW, workflowName, workflowRun);
      return RuntimeMonitors.isWellFormed(parentWorkflowRunId) ? parentWorkflowRunId : runId;
    } catch (Exception e) {
      LOG.debug("Ignoring malformed parent workflow coordinates for run {}: workflow={}, run={}",
          runId, workflowName, workflowRun, e);
      return runId;
    }
  }

  @Nullable
  private static RuntimeScopes parseRuntimeScopes(ProgramRunId runId, @Nullable String rawScopes) {
    if (rawScopes == null) {
      return null;
    }
    try {
      return RuntimeScopes.fromJson(rawScopes);
    } catch (JsonParseException e) {
      LOG.warn("Failed to parse {} for program run {}",
          ProgramOptionConstants.RUNTIME_SCOPES, runId, e);
      return null;
    }
  }

  private static ProgramRunInfo buildRunInfo(RunRecordDetail runRecord) {
    ProgramRunStatus programRunStatus = runRecord.getStatus();
    if (programRunStatus != ProgramRunStatus.STOPPING) {
      return new ProgramRunInfo(programRunStatus);
    }
    // For stopping state, encode the termination timestamp in seconds
    long terminateTs = Optional.ofNullable(runRecord.getTerminateTs()).orElse(Long.MAX_VALUE);
    return new ProgramRunInfo(terminateTs);
  }

  /**
   * Inserts an active fetched {@link RunRecordDetail} for the program run into the runtime store.
   */
  private void insertRunRecord(ProgramRunId programRunId, RunRecordDetail runRecord) {
    try {
      TransactionRunners.run(txRunner, context -> {
        AppMetadataStore store = AppMetadataStore.create(context);
        // Strip off user args and trim down system args as runtime only needs the run status for validation purpose.
        // User and system args could be large and store them in local store can lead to unnecessary storage
        // and processing overhead.
        store.recordProgramProvisioning(programRunId, Collections.emptyMap(),
            RuntimeMonitors.trimSystemArgs(runRecord.getSystemArgs()),
            runRecord.getSourceId(), runRecord.getArtifactId());
        store.recordProgramProvisioned(programRunId, 1, runRecord.getSourceId());
        store.recordProgramStart(programRunId, null, runRecord.getSystemArgs(),
            runRecord.getSourceId());
        if (runRecord.getStatus() == ProgramRunStatus.STARTING) {
          return;
        }
        store.recordProgramRunning(programRunId,
            MoreObjects.firstNonNull(runRecord.getRunTs(), System.currentTimeMillis()),
            null, runRecord.getSourceId());
        switch (runRecord.getStatus()) {
          case SUSPENDED:
            store.recordProgramSuspend(programRunId, runRecord.getSourceId(),
                MoreObjects.firstNonNull(runRecord.getSuspendTs(), System.currentTimeMillis()));
            break;
          case STOPPING:
            store.recordProgramStopping(programRunId, runRecord.getSourceId(),
                MoreObjects.firstNonNull(runRecord.getStoppingTs(), System.currentTimeMillis()),
                // if terminate timestamp is null we will shut down gracefully
                MoreObjects.firstNonNull(runRecord.getTerminateTs(), Long.MAX_VALUE));
            break;
          default:
            break;
        }
      }, IOException.class);
    } catch (Exception e) {
      // Don't throw if failed to update to the store. It doesn't affect normal operation.
      LOG.warn("Failed to update runtime store for program run {} with {}", programRunId, runRecord,
          e);
    }
  }

  private static String stripQuery(@Nullable String uri) {
    if (uri == null) {
      return "";
    }
    int cut = uri.length();
    int queryIdx = uri.indexOf('?');
    if (queryIdx >= 0) {
      cut = Math.min(cut, queryIdx);
    }
    int fragmentIdx = uri.indexOf('#');
    if (fragmentIdx >= 0) {
      cut = Math.min(cut, fragmentIdx);
    }
    return uri.substring(0, cut);
  }

  private enum LookupState {
    NOT_FOUND,
    NOT_LAUNCHED,
    ACTIVE,
    FINISHED_LOCAL,
    FINISHED_FETCHED
  }

  private static final class RunLookupResult {

    private final LookupState state;
    @Nullable
    private final ProgramRunInfo programRunInfo;
    @Nullable
    private final ProgramRunId topLevelRun;
    private final boolean hasScopesArg;
    @Nullable
    private final RuntimeScopes runtimeScopes;

    private RunLookupResult(LookupState state, @Nullable ProgramRunInfo programRunInfo,
        @Nullable ProgramRunId topLevelRun, boolean hasScopesArg,
        @Nullable RuntimeScopes runtimeScopes) {
      this.state = state;
      this.programRunInfo = programRunInfo;
      this.topLevelRun = topLevelRun;
      this.hasScopesArg = hasScopesArg;
      this.runtimeScopes = runtimeScopes;
    }

    static RunLookupResult notFound() {
      return new RunLookupResult(LookupState.NOT_FOUND, null, null, false, null);
    }

    LookupState getState() {
      return state;
    }

    @Nullable
    ProgramRunInfo getProgramRunInfo() {
      return programRunInfo;
    }

    @Nullable
    ProgramRunId getTopLevelRun() {
      return topLevelRun;
    }

    boolean hasScopesArg() {
      return hasScopesArg;
    }

    @Nullable
    RuntimeScopes getRuntimeScopes() {
      return runtimeScopes;
    }
  }
}
