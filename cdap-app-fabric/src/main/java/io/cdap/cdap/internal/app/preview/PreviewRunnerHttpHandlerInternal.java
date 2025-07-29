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
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.BodyProducer;
import io.cdap.http.HttpResponder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import javax.annotation.Nullable;
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

  /**
   * Holds the total number of requests that have been executed by this handler that should count
   * toward max allowed.
   */
  private final AtomicInteger runningRequestCount = new AtomicInteger(0);
  private final AtomicInteger requestProcessedCount = new AtomicInteger(0);
  private final CConfiguration cConf;
  private final int concurrentRequestLimit;
  private final PreviewRunner previewRunner;
  private final BiConsumer<Boolean, PreviewRequestDetails> previewCompletionConsumer;
  private final PreviewRequestPollerInfoProvider pollerInfoProvider;
  /**
   * If true, pod will restart once an operation finish its execution.
   */
  private final AtomicBoolean mustRestart = new AtomicBoolean(false);

  public PreviewRunnerHttpHandlerInternal(CConfiguration cConf, PreviewRunner previewRunner,
      Consumer<ProgramId> stopper, PreviewRequestPollerInfoProvider pollerInfoProvider) {
    this.cConf = cConf;
    this.concurrentRequestLimit = 1;
    this.previewRunner = previewRunner;
    this.pollerInfoProvider = pollerInfoProvider;
    // Restart the service to clean up and re-claim resources after user code
    // execution.
    this.previewCompletionConsumer = (succeeded, previewRequestDetails) -> {
      final int pendingRequests = runningRequestCount.decrementAndGet();
      requestProcessedCount.incrementAndGet();

      PreviewRequest previewRequest = previewRequestDetails.getRequest();
      LOG.info("sidhdirenge - it came here");
      if (mustRestart.get() && pendingRequests == 0) {
        stopper.accept(previewRequest.getProgram());
        return;
      }

      //TODO(sidhdirenge):Add this as well
//      if (requestProcessedCount.get() >= killAfterRequestCount) {
//        stopper.accept(previewRequest.getProgram().getParent());
//      }
    };

  }

  @POST
  @Path("/run")
  public void run(FullHttpRequest request, HttpResponder responder) {
    LOG.info("sidhdirenge - Run called");
    if (mustRestart.get()) {
      LOG.info("sidhdirenge - too many requests part 1");
      responder.sendStatus(HttpResponseStatus.TOO_MANY_REQUESTS);
      return;
    }
    if (runningRequestCount.incrementAndGet() > concurrentRequestLimit) {
      LOG.info("sidhdirenge - too many requests part 2");
      responder.sendStatus(HttpResponseStatus.TOO_MANY_REQUESTS);
      runningRequestCount.decrementAndGet();
      return;
    }
    PreviewRequest previewRequest = GSON.fromJson(
        request.content().toString(StandardCharsets.UTF_8),
        PreviewRequest.class);
    byte[] pollerInfo = pollerInfoProvider.get();
    PreviewRequestDetails previewRequestDetails = new PreviewRequestDetails(previewRequest, pollerInfo);
//    previewStore.setPreviewRequestPollerInfo(previewRequest.getProgram().getParent(),
//        pollerInfo);
    try {
      LOG.info("sidhdirenge - Preview called");
      previewRunner.startPreview(previewRequest);
      LOG.info("sidhdirenge - startPreview() complete");
    } catch (Exception e) {
      LOG.error("sidhdirenge - Exception", e);
      throw new RuntimeException(e);
    }
    responder.sendContent(HttpResponseStatus.OK,
        new PreviewBodyProducer(previewCompletionConsumer, previewRequestDetails),
        new DefaultHttpHeaders().add(HttpHeaders.CONTENT_TYPE,
            MediaType.APPLICATION_JSON));
  }

  /**
   * By using BodyProducer instead of simply sending out response bytes, the handler can get
   * notified (through finished method) when sending the response is done, so it can safely call the
   * stopper to kill the worker pod.
   */
  private static class PreviewBodyProducer extends BodyProducer {

    private final BiConsumer<Boolean, PreviewRequestDetails> previewRequestDetailsBiConsumer;
    private final PreviewRequestDetails previewRequestDetails;
    private final AtomicBoolean sent = new AtomicBoolean(false);

    PreviewBodyProducer(BiConsumer<Boolean, PreviewRequestDetails> previewRequestDetailsBiConsumer,
        PreviewRequestDetails previewRequestDetails) {
      this.previewRequestDetailsBiConsumer = previewRequestDetailsBiConsumer;
      this.previewRequestDetails = previewRequestDetails;
    }

    @Override
    public ByteBuf nextChunk() {
      // Send the content only once. Subsequent calls will return EMPTY_BUFFER.
      if (!sent.compareAndSet(false, true)) {
        return Unpooled.EMPTY_BUFFER;
      }
      return Unpooled.copiedBuffer(GSON.toJson(previewRequestDetails), StandardCharsets.UTF_8);
    }

    @Override
    public void finished() {
      LOG.info("sidhdirenge - producer.finished()");
      previewRequestDetailsBiConsumer.accept(true, previewRequestDetails);
    }

    @Override
    public void handleError(@Nullable Throwable cause) {
      LOG.error("sidhdirenge - Error when sending chunks", cause);
      previewRequestDetailsBiConsumer.accept(false, previewRequestDetails);
    }
  }
}
