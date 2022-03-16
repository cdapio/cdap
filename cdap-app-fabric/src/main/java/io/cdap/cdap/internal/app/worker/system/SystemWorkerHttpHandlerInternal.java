/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.worker.system;

import com.google.inject.Injector;
import com.google.inject.Singleton;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.worker.AbstractWorkerHttpHandlerInternal;
import io.cdap.cdap.internal.app.worker.TaskDetails;
import io.cdap.http.BodyProducer;
import io.cdap.http.HttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.EmptyHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nullable;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

/**
 * Internal {@link HttpHandler} for System worker.
 */
@Singleton
@Path(Constants.Gateway.INTERNAL_API_VERSION_3 + "/system")
public class SystemWorkerHttpHandlerInternal extends AbstractWorkerHttpHandlerInternal {

  private static final Logger LOG = LoggerFactory.getLogger(SystemWorkerHttpHandlerInternal.class);
  private final int requestLimit;

  public SystemWorkerHttpHandlerInternal(CConfiguration cConf,
      MetricsCollectionService metricsCollectionService, Injector injector) {
    super(metricsCollectionService, injector);
    this.requestLimit = cConf.getInt(Constants.SystemWorker.REQUEST_LIMIT);
  }

  @POST
  @Path("/run")
  public void run(FullHttpRequest request, HttpResponder responder) {
    if (requestProcessedCount.incrementAndGet() > requestLimit) {
      responder.sendStatus(HttpResponseStatus.TOO_MANY_REQUESTS);
      return;
    }

    long startTime = System.currentTimeMillis();
    String className;
    try {
      RunnableTaskRequest runnableTaskRequest = getRunnableTaskRequest(request);
      className = getTaskClassName(runnableTaskRequest);
      RunnableTaskContext runnableTaskContext = launchRunnableTask(runnableTaskRequest);
      TaskDetails taskDetails = new TaskDetails(true, className, startTime);
      emitMetrics(taskDetails, Constants.Metrics.SystemWorker.REQUEST_COUNT,
          Constants.Metrics.SystemWorker.REQUEST_LATENCY_MS);
      responder.sendContent(HttpResponseStatus.OK,
          new RunnableTaskBodyProducer(runnableTaskContext),
          new DefaultHttpHeaders().add(HttpHeaders.CONTENT_TYPE,
              MediaType.APPLICATION_OCTET_STREAM));
    } catch (ClassNotFoundException | ClassCastException ex) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, exceptionToJson(ex),
          EmptyHttpHeaders.INSTANCE);
    } catch (Exception ex) {
      LOG.error("Failed to run task {}", request.content().toString(StandardCharsets.UTF_8), ex);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, exceptionToJson(ex),
          EmptyHttpHeaders.INSTANCE);
    }
    requestProcessedCount.decrementAndGet();
  }

  private static class RunnableTaskBodyProducer extends BodyProducer {

    private final ByteBuffer response;
    private boolean done = false;

    RunnableTaskBodyProducer(RunnableTaskContext context) {
      this.response = context.getResult();
    }

    @Override
    public ByteBuf nextChunk() {
      if (done) {
        return Unpooled.EMPTY_BUFFER;
      }
      done = true;
      return Unpooled.wrappedBuffer(response);
    }

    @Override
    public void finished() {
    }

    @Override
    public void handleError(@Nullable Throwable cause) {
      LOG.error("Error when sending chunks", cause);
    }
  }
}
