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

package io.cdap.cdap.gateway.handlers.preview;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.app.preview.PreviewManager;
import io.cdap.cdap.app.preview.PreviewStatus.Status;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.logging.ErrorLogsClassifier;
import io.cdap.cdap.logging.context.LoggingContextHelper;
import io.cdap.cdap.logging.filter.Filter;
import io.cdap.cdap.logging.filter.FilterParser;
import io.cdap.cdap.logging.read.LogEvent;
import io.cdap.cdap.logging.read.LogOffset;
import io.cdap.cdap.logging.read.LogReader;
import io.cdap.cdap.logging.read.ReadRange;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * v3 {@link io.cdap.http.HttpHandler} to handle /classify requests for preview runs
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3)
public class PreviewErrorClassificationHttpHandler extends PreviewHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(
      PreviewErrorClassificationHttpHandler.class);
  private final ErrorLogsClassifier errorLogsClassifier;

  /**
   * Constructor for PreviewErrorClassificationHttpHandler.
   */
  @Inject
  public PreviewErrorClassificationHttpHandler(PreviewManager previewManager,
      CConfiguration cConf, ErrorLogsClassifier errorLogsClassifier) {
    super(previewManager, cConf);
    this.errorLogsClassifier = errorLogsClassifier;
  }

  private ProgramRunId getProgramRunId(ApplicationId applicationId) throws Exception {
    ProgramRunId programRunId = previewManager.getRunId(applicationId);
    if (programRunId == null) {
      throw new NotFoundException(
          String.format("No run record found for previewId: %s in namespace: %s",
              applicationId.getApplication(), applicationId.getNamespace()));
    }
    return programRunId;
  }

  /**
   * Returns the list of {@link io.cdap.cdap.proto.ErrorClassificationResponse} for
   * failed preview run.
   */
  @POST
  @Path("/namespaces/{namespace-id}/previews/{preview-id}/classify")
  public void classifyRunIdLogs(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @PathParam("preview-id") String previewId) throws Exception {
    ApplicationId applicationId = new ApplicationId(namespaceId, previewId);
    ProgramRunId programRunId = getProgramRunId(applicationId);
    if (previewManager.getStatus(applicationId).getStatus() != Status.RUN_FAILED) {
      throw new IllegalArgumentException("Classification is only supported "
          + "for failed preview runs");
    }
    LoggingContext loggingContext = LoggingContextHelper.getLoggingContextWithRunId(programRunId,
        null);
    LogReader logReader = previewManager.getLogReader();

    Filter filter = FilterParser.parse("loglevel=ERROR");
    ReadRange readRange = new ReadRange(0, System.currentTimeMillis(),
        LogOffset.INVALID_KAFKA_OFFSET);
    readRange = adjustReadRange(readRange, null, true);
    try (CloseableIterator<LogEvent> logIter = logReader.getLog(loggingContext,
        readRange.getFromMillis(), readRange.getToMillis(), filter)) {
      // the iterator is closed by the BodyProducer passed to the HttpResponder
      errorLogsClassifier.classify(logIter, responder, namespaceId, null, "preview", previewId);
    } catch (Exception ex) {
      LOG.debug("Exception while classifying logs for logging context {}", loggingContext, ex);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }
}
