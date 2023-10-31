/*
 * Copyright © 2022-2023 Cask Data, Inc.
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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.RunnableTaskLauncher;
import io.cdap.cdap.common.internal.remote.TaskDetails;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.codec.BasicThrowableCodec;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.security.ApplicationPermission;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.BodyProducer;
import io.cdap.http.HttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.EmptyHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Internal {@link HttpHandler} for System worker.
 */
@Singleton
@Path(Constants.Gateway.INTERNAL_API_VERSION_3 + "/system")
public class SystemWorkerHttpHandlerInternal extends AbstractHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(SystemWorkerHttpHandlerInternal.class);
  private static final Gson GSON = new GsonBuilder().registerTypeAdapter(BasicThrowable.class,
      new BasicThrowableCodec()).create();
  private final int requestLimit;

  private final RunnableTaskLauncher runnableTaskLauncher;

  /**
   * Holds the total number of requests that have been executed by this handler that should count
   * toward max allowed.
   */
  private final AtomicInteger requestProcessedCount = new AtomicInteger(0);

  private final MetricsCollectionService metricsCollectionService;
  private final AuthenticationContext authenticationContext;
  private final AccessEnforcer internalAccessEnforcer;

  public SystemWorkerHttpHandlerInternal(CConfiguration cConf,
      MetricsCollectionService metricsCollectionService,
      Injector injector, AuthenticationContext authenticationContext,
      AccessEnforcer internalAccessEnforcer) {
    this.runnableTaskLauncher = new RunnableTaskLauncher(injector);
    this.metricsCollectionService = metricsCollectionService;
    this.requestLimit = cConf.getInt(Constants.SystemWorker.REQUEST_LIMIT);
    this.authenticationContext = authenticationContext;
    this.internalAccessEnforcer = internalAccessEnforcer;
  }

  @POST
  @Path("/run")
  public void run(FullHttpRequest request, HttpResponder responder) {
    // Perform a permission check to ensure the caller is part of the system, then unset the SecurityRequestContext to
    // force the system worker to use its system context.
    internalAccessEnforcer.enforce(InstanceId.SELF, authenticationContext.getPrincipal(),
        ApplicationPermission.EXECUTE);
    SecurityRequestContext.reset();

    if (requestProcessedCount.incrementAndGet() > requestLimit) {
      responder.sendStatus(HttpResponseStatus.TOO_MANY_REQUESTS);
      requestProcessedCount.decrementAndGet();
      return;
    }

    long startTime = System.currentTimeMillis();
    RunnableTaskRequest runnableTaskRequest = GSON.fromJson(
        request.content().toString(StandardCharsets.UTF_8),
        RunnableTaskRequest.class);
    RunnableTaskContext runnableTaskContext = new RunnableTaskContext(runnableTaskRequest);
    try {
      runnableTaskLauncher.launchRunnableTask(runnableTaskContext);
    } catch (Exception e) {
      new TaskDetails(metricsCollectionService, startTime, false, null).emitMetrics(false);

      if (e instanceof ClassNotFoundException || e instanceof ClassCastException) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, exceptionToJson(e),
            EmptyHttpHeaders.INSTANCE);
      } else {
        LOG.error("Failed to run task", e);
        responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, exceptionToJson(e),
            EmptyHttpHeaders.INSTANCE);
      }

      runnableTaskContext.executeCleanupTask();
      requestProcessedCount.decrementAndGet();
      return;
    }

    TaskDetails taskDetails = new TaskDetails(metricsCollectionService, startTime,
        runnableTaskContext.isTerminateOnComplete(), runnableTaskRequest);
    responder.sendContent(HttpResponseStatus.OK,
        new RunnableTaskBodyProducer(runnableTaskContext, taskDetails),
        new DefaultHttpHeaders().add(HttpHeaders.CONTENT_TYPE,
            MediaType.APPLICATION_OCTET_STREAM));
  }

  /**
   * Return json representation of an exception. Used to propagate exception across network for
   * better surfacing errors and debuggability.
   */
  private String exceptionToJson(Exception ex) {
    BasicThrowable basicThrowable = new BasicThrowable(ex);
    return GSON.toJson(basicThrowable);
  }

  private class RunnableTaskBodyProducer extends BodyProducer {

    private final RunnableTaskContext context;
    private final TaskDetails taskDetails;
    private boolean done;

    RunnableTaskBodyProducer(RunnableTaskContext context, TaskDetails taskDetails) {
      this.context = context;
      this.taskDetails = taskDetails;
    }

    @Override
    public ByteBuf nextChunk() {
      if (done) {
        return Unpooled.EMPTY_BUFFER;
      }

      done = true;
      return Unpooled.wrappedBuffer(context.getResult());
    }

    @Override
    public void finished() {
      context.executeCleanupTask();
      taskDetails.emitMetrics(true);
      requestProcessedCount.decrementAndGet();
    }

    @Override
    public void handleError(@Nullable Throwable cause) {
      LOG.error("Error when sending chunks", cause);
      context.executeCleanupTask();
      taskDetails.emitMetrics(false);
      requestProcessedCount.decrementAndGet();
    }
  }
}
