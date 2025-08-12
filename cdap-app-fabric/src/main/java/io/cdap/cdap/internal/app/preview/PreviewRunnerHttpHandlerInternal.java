package io.cdap.cdap.internal.app.preview;

import com.google.gson.Gson;
import com.google.inject.Singleton;
import io.cdap.cdap.app.preview.PreviewRequest;
import io.cdap.cdap.app.preview.PreviewRunner;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@Path(Constants.Gateway.INTERNAL_API_VERSION_3 + "/preview-runner")
public class PreviewRunnerHttpHandlerInternal extends AbstractHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(PreviewRunnerHttpHandlerInternal.class);
  private static final Gson GSON = new Gson();

  private final AtomicInteger runningRequestCount = new AtomicInteger(0);
  private final AtomicInteger requestProcessedCount = new AtomicInteger(0);
  private final CConfiguration cConf;
  private final int concurrentRequestLimit;
  private final PreviewRunner previewRunner;
  private final BiConsumer<Boolean, PreviewRequestDetails> previewCompletionConsumer;
  private final PreviewRequestPollerInfoProvider pollerInfoProvider;
  private final AtomicBoolean mustRestart = new AtomicBoolean(false);

  public PreviewRunnerHttpHandlerInternal(CConfiguration cConf, PreviewRunner previewRunner,
      Consumer<ProgramId> stopper, PreviewRequestPollerInfoProvider pollerInfoProvider) {
    this.cConf = cConf;
    this.concurrentRequestLimit = 1; // Assuming this remains 1
    this.previewRunner = previewRunner;
    this.pollerInfoProvider = pollerInfoProvider;

    // This consumer will now be called explicitly on pipeline completion,
    // not from the BodyProducer's finished() method.
    this.previewCompletionConsumer = (succeeded, previewRequestDetails) -> {
      final int pendingPipelines = runningRequestCount.decrementAndGet(); // Decrement here, linked to pipeline
      requestProcessedCount.incrementAndGet(); // Increment here, linked to pipeline

      PreviewRequest previewRequest = previewRequestDetails.getRequest();
      LOG.info(
          "sidhdirenge - Pipeline completed for preview {}. Succeeded: {}. Pending pipelines: {}",
          previewRequest.getProgram(), succeeded, pendingPipelines);

//      if (mustRestart.get() && pendingPipelines == 0) {
      LOG.info(
          "sidhdirenge - All pipelines finished and mustRestart is true. Stopping pod for preview {}.",
          previewRequest.getProgram());
      stopper.accept(previewRequest.getProgram());
//        return;
//      }
      // Add other pod killing logic based on killAfterRequestCount here if needed,
      // also linked to `requestProcessedCount` (completed pipelines).
    };
  }

  @POST
  @Path("/run")
  public void run(FullHttpRequest request, HttpResponder responder) {
    LOG.info("sidhdirenge - Run called");

    if (mustRestart.get()) {
      LOG.info("sidhdirenge - too many requests part 1 (mustRestart)");
      responder.sendStatus(HttpResponseStatus.TOO_MANY_REQUESTS);
      return;
    }
    if (runningRequestCount.incrementAndGet() > concurrentRequestLimit) {
      LOG.info("sidhdirenge - too many requests part 2 (concurrency limit)");
      responder.sendStatus(HttpResponseStatus.TOO_MANY_REQUESTS);
      runningRequestCount.decrementAndGet(); // Decrement immediately if rejected
      return;
    }

    PreviewRequest previewRequest = GSON.fromJson(
        request.content().toString(StandardCharsets.UTF_8), PreviewRequest.class);
    byte[] pollerInfo = pollerInfoProvider.get();
    PreviewRequestDetails previewRequestDetails = new PreviewRequestDetails(previewRequest,
        pollerInfo);

    try {
      LOG.info("sidhdirenge - Initiating preview for program {}", previewRequest.getProgram());
      // This future completes AFTER the pipeline execution is done
      CompletableFuture<PreviewRequest> pipelineCompletionFuture =
          (CompletableFuture<PreviewRequest>) previewRunner.startPreview(
              previewRequest);
      LOG.info("sidhdirenge - startPreview() call returned for program {}",
          previewRequest.getProgram());

      // Attach the external completion consumer to the pipeline's future.
      // This consumer will now be invoked when the pipeline itself finishes.
      pipelineCompletionFuture.whenComplete((result, ex) -> {
        // This code runs on a non-Netty thread from CompletableFuture's default executor.
        // It triggers the main pipeline completion logic.
        if (ex == null) {
          previewCompletionConsumer.accept(true, previewRequestDetails);
        } else {
          LOG.error("sidhdirenge - Pipeline execution failed for preview {}: {}",
              previewRequest.getProgram(), ex.getMessage(), ex);
          previewCompletionConsumer.accept(false, previewRequestDetails);
        }
      });

      // --- IMMEDIATE HTTP RESPONSE ---
      // Send an HTTP OK response immediately, but with an empty body or a simple status string.
      // The client is now informed that the request was received and the preview started.
      // The actual preview details will be published via other means (e.g., messaging service).
      String responseBodyJson = GSON.toJson(Collections.singletonMap("status",
          "Preview initiation successful. Pipeline execution in progress."));

      responder.sendString(HttpResponseStatus.OK, responseBodyJson,
          new DefaultHttpHeaders().add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON));

      LOG.info("sidhdirenge - HTTP OK response sent for preview {}", previewRequest.getProgram());

    } catch (Exception e) {
      LOG.error("sidhdirenge - Exception initiating preview for program {}",
          previewRequest.getProgram(), e);
      runningRequestCount.decrementAndGet(); // Decrement count on initiation error
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
          "Failed to initiate preview: " + e.getMessage());
    }
  }
}