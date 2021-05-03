/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.worker;

import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.logging.gateway.handlers.AbstractLogHttpHandler;
import io.cdap.http.HttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.EmptyHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * Internal {@link HttpHandler} for Task worker.
 */
@Singleton
@Path(Constants.Gateway.INTERNAL_API_VERSION_3 + "/worker")
public class TaskWorkerHttpHandlerInternal extends AbstractLogHttpHandler {
  public static final String UNKNOWN_CLASS_REQUEST = "UNKNOWN_REQUEST";
  private static final Logger LOG = LoggerFactory.getLogger(TaskWorkerHttpHandlerInternal.class);
  private static final Gson GSON = new Gson();
  private final RunnableTaskLauncher runnableTaskLauncher;
  private final Consumer<String> stopper;
  private final AtomicInteger inflightRequests = new AtomicInteger(0);

  @Inject
  public TaskWorkerHttpHandlerInternal(CConfiguration cConf, Consumer<String> stopper) {
    super(cConf);
    runnableTaskLauncher = new RunnableTaskLauncher(cConf);
    this.stopper = stopper;
  }

  @POST
  @Path("/run")
  public void run(FullHttpRequest request, HttpResponder responder) {
    byte[] response;
    HttpResponseStatus status;
    String className = UNKNOWN_CLASS_REQUEST;
    try {
      if (inflightRequests.incrementAndGet() > 1) {
        responder.sendStatus(HttpResponseStatus.TOO_MANY_REQUESTS);
        return;
      }
      RunnableTaskRequest runnableTaskRequest =
        GSON.fromJson(request.content().toString(StandardCharsets.UTF_8), RunnableTaskRequest.class);
      response = runnableTaskLauncher.launchRunnableTask(runnableTaskRequest);
      status = HttpResponseStatus.OK;
      className = runnableTaskRequest.className;
    } catch (Exception ex) {
      LOG.error(String.format("failed to handle request %s",
                              request.content().toString(StandardCharsets.UTF_8), ex));
      response = ex.toString().getBytes();
      status = HttpResponseStatus.BAD_REQUEST;
    }
    responder.sendByteArray(status, response, EmptyHttpHeaders.INSTANCE);

    stopper.accept(className);
  }

}
