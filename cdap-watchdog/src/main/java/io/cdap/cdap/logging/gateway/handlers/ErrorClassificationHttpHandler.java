/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.logging.gateway.handlers;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.logging.ErrorLogsClassifier;
import io.cdap.cdap.logging.context.LoggingContextHelper;
import io.cdap.cdap.logging.filter.Filter;
import io.cdap.cdap.logging.filter.FilterParser;
import io.cdap.cdap.logging.read.LogEvent;
import io.cdap.cdap.logging.read.LogOffset;
import io.cdap.cdap.logging.read.LogReader;
import io.cdap.cdap.logging.read.ReadRange;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramReference;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.http.HttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * v3 {@link HttpHandler} to handle /classify requests for program runs
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3)
public class ErrorClassificationHttpHandler extends AbstractLogHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ErrorClassificationHttpHandler.class);
  private final LogReader logReader;
  private final ProgramRunRecordFetcher programRunRecordFetcher;
  private final AccessEnforcer accessEnforcer;
  private final AuthenticationContext authenticationContext;
  private final ErrorLogsClassifier errorLogsClassifier;

  /**
   * Constructor for ErrorClassificationHttpHandler.
   */
  @Inject
  public ErrorClassificationHttpHandler(AccessEnforcer accessEnforcer,
      AuthenticationContext authenticationContext,
      LogReader logReader,
      ProgramRunRecordFetcher programRunFetcher,
      ErrorLogsClassifier errorLogsClassifier,
      CConfiguration cConf) {
    super(cConf);
    this.logReader = logReader;
    this.programRunRecordFetcher = programRunFetcher;
    this.accessEnforcer = accessEnforcer;
    this.authenticationContext = authenticationContext;
    this.errorLogsClassifier = errorLogsClassifier;
  }


  private RunRecordDetail getRunRecordMeta(ProgramReference programRef, String runId)
      throws IOException, NotFoundException, UnauthorizedException {
    RunRecordDetail runRecordMeta = programRunRecordFetcher.getRunRecordMeta(programRef, runId);
    if (runRecordMeta == null) {
      throw new NotFoundException(
          String.format("No run record found for program %s and runID: %s", programRef, runId));
    }
    return runRecordMeta;
  }

  private void ensureVisibilityOnProgram(String namespace, String application, String programType,
      String program) {
    ApplicationId appId = new ApplicationId(namespace, application);
    ProgramId programId = new ProgramId(appId, ProgramType.valueOfCategoryName(programType),
        program);
    accessEnforcer.enforce(programId, authenticationContext.getPrincipal(), StandardPermission.GET);
  }

  /**
   * Returns the list of {@link io.cdap.cdap.proto.ErrorClassificationResponse} for
   * failed program run.
   */
  @POST
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/runs/{run-id}/classify")
  public void classifyRunIdLogs(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @PathParam("app-id") String appId, @PathParam("program-type") String programType,
      @PathParam("program-id") String programId,
      @PathParam("run-id") String runId) throws Exception {
    ensureVisibilityOnProgram(namespaceId, appId, programType, programId);
    ProgramType type = ProgramType.valueOfCategoryName(programType);
    ProgramReference programRef = new ProgramReference(namespaceId, appId, type, programId);
    RunRecordDetail runRecord = getRunRecordMeta(programRef, runId);
    if (runRecord.getStatus() != ProgramRunStatus.FAILED) {
      throw new IllegalArgumentException("Classification is only supported for failed runs");
    }
    LoggingContext loggingContext = LoggingContextHelper.getLoggingContextWithRunId(programRef,
        runId, runRecord.getSystemArgs());

    Filter filter = FilterParser.parse("loglevel=ERROR");
    ReadRange readRange = new ReadRange(0, System.currentTimeMillis(),
        LogOffset.INVALID_KAFKA_OFFSET);
    readRange = adjustReadRange(readRange, runRecord, true);
    try (CloseableIterator<LogEvent> logIter = logReader.getLog(loggingContext,
        readRange.getFromMillis(), readRange.getToMillis(), filter)) {
      // the iterator is closed by the BodyProducer passed to the HttpResponder
      errorLogsClassifier.classify(logIter, responder, namespaceId, programId, appId, runId);
    } catch (Exception ex) {
      LOG.debug("Exception while classifying logs for logging context {}", loggingContext, ex);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }
}
