/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.preview;

import com.google.gson.Gson;
import com.google.inject.Singleton;
import io.cdap.cdap.app.preview.PreviewRequest;
import io.cdap.cdap.app.preview.PreviewRunner;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.EmptyHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.HttpHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@Path(Constants.Gateway.INTERNAL_API_VERSION_3 + "/" + Constants.Service.PREVIEW_RUNNER)
public class PreviewRunnerHttpHandlerInternal extends AbstractHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(PreviewRunnerHttpHandlerInternal.class);
  private static final Gson GSON = new Gson();
  private final AtomicInteger runningRequestCount = new AtomicInteger(0);
  private final int concurrentRequestLimit;
  private final PreviewRequestPollerInfoProvider pollerInfoProvider;
  private final PreviewRunner previewRunner;
  private final Consumer<ProgramId> previewCompletionConsumer;

  public PreviewRunnerHttpHandlerInternal(int concurrentRequestLimit,
      PreviewRequestPollerInfoProvider pollerInfoProvider, PreviewRunner previewRunner,
      Consumer<ProgramId> stopper) {
    //TODO(sidhdirenge): Fetch from cConf.
    this.concurrentRequestLimit = concurrentRequestLimit;
    this.pollerInfoProvider = pollerInfoProvider;
    this.previewRunner = previewRunner;
    // Restart the service to clean up and re-claim resources after user code
    // execution.
    this.previewCompletionConsumer = (previewApp) -> {
      final int pendingRequests = runningRequestCount.decrementAndGet();
      if (pendingRequests == 0) {
        stopper.accept(previewApp);
      }
    };
  }

  @POST
  @Path("/run")
  public void run(FullHttpRequest request, HttpResponder responder) {
    if (runningRequestCount.incrementAndGet() > concurrentRequestLimit) {
      responder.sendStatus(HttpResponseStatus.TOO_MANY_REQUESTS);
      runningRequestCount.decrementAndGet();
      return;
    }
    PreviewRequest previewRequest = GSON.fromJson(
        request.content().toString(StandardCharsets.UTF_8), PreviewRequest.class);
    try {
      LOG.info("Initiating preview for program {}", previewRequest.getProgram());

      // This future completes AFTER the pipeline execution is done.
      // So that consumer knows when to shut down runner pod.
      CompletableFuture<PreviewRequest> future = (CompletableFuture<PreviewRequest>) previewRunner.startPreview(
          previewRequest);
      future.whenComplete((result, e) -> {
        if (e != null) {
          // Just log a debug if preview failed since it is expected for an application having execution failure
          LOG.error("Pipeline execution failed for preview {}", previewRequest.getProgram(), e);
        }
        previewCompletionConsumer.accept(previewRequest.getProgram());
      });

      byte[] pollerInfo = pollerInfoProvider.get();
      // Send an HTTP OK response immediately, but with an empty body or a simple status string.
      // The client is now informed that the request was received and the preview started.
      responder.sendByteArray(HttpResponseStatus.OK, pollerInfo, EmptyHttpHeaders.INSTANCE);
    } catch (Exception e) {
      LOG.error("Exception initiating preview for program {}", previewRequest.getProgram(), e);
      runningRequestCount.decrementAndGet();
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, exceptionToJson(e),
          new DefaultHttpHeaders().set(HttpHeaders.CONTENT_TYPE, "application/json"));
    }
  }

  /**
   * Return json representation of an exception. Used to propagate exception across network for
   * better surfacing errors and debugging.
   */
  private String exceptionToJson(Exception ex) {
    BasicThrowable basicThrowable = new BasicThrowable(ex);
    return GSON.toJson(basicThrowable);
  }
}