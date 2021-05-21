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

import com.google.common.io.Closeables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.logging.gateway.handlers.AbstractLogHttpHandler;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.codec.BasicThrowableCodec;
import io.cdap.http.BodyProducer;
import io.cdap.http.HttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.EmptyHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

/**
 * Internal {@link HttpHandler} for Task worker.
 */
@Singleton
@Path(Constants.Gateway.INTERNAL_API_VERSION_3 + "/worker")
public class TaskWorkerHttpHandlerInternal extends AbstractLogHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(TaskWorkerHttpHandlerInternal.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(BasicThrowable.class, new BasicThrowableCodec()).create();
  private final RunnableTaskLauncher runnableTaskLauncher;
  private final Consumer<String> stopper;

  @Inject
  public TaskWorkerHttpHandlerInternal(CConfiguration cConf, Configuration hConf, Consumer<String> stopper) {
    super(cConf);
    runnableTaskLauncher = new RunnableTaskLauncher(cConf, hConf);
    this.stopper = stopper;
  }

  @POST
  @Path("/run")
  public void run(FullHttpRequest request, HttpResponder responder) {
    String className = null;
    CountDownLatch latch = new CountDownLatch(1);
    try {
      RunnableTaskRequest runnableTaskRequest =
        GSON.fromJson(request.content().toString(StandardCharsets.UTF_8), RunnableTaskRequest.class);
      className = runnableTaskRequest.getClassName();
      byte[] response = runnableTaskLauncher.launchRunnableTask(runnableTaskRequest);

      InputStream responseStream = new ByteArrayInputStream(response);

      responder.sendContent(HttpResponseStatus.OK, new BodyProducer() {
        @Override
        public ByteBuf nextChunk() throws Exception {
          byte[] buf = new byte[64 * 1024];
          int len = responseStream.read(buf);
          if (len == -1) {
            return Unpooled.EMPTY_BUFFER;
          }
          return Unpooled.wrappedBuffer(buf, 0, len);
        }

        @Override
        public void finished() {
          Closeables.closeQuietly(responseStream);
          latch.countDown();
        }

        @Override
        public void handleError(@Nullable Throwable cause) {
          LOG.error("Error when sending chunks", cause);
        }
      }, new DefaultHttpHeaders().add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_OCTET_STREAM));
    } catch (ClassNotFoundException | ClassCastException ex) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, exceptionToJson(ex), EmptyHttpHeaders.INSTANCE);
      latch.countDown();
    } catch (Exception ex) {
      LOG.error(String.format("Failed to run task %s",
                              request.content().toString(StandardCharsets.UTF_8)), ex);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, exceptionToJson(ex), EmptyHttpHeaders.INSTANCE);
      latch.countDown();
    } finally {
      try {
        latch.await(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        LOG.error("Waiting for sending task worker response failed ", e);
      }
      stopper.accept(className);
    }
  }

  /**
   * Return json representation of an exception.
   * Used to propagate exception across network for better surfacing errors and debuggability.
   */
  private String exceptionToJson(Exception ex) {
    BasicThrowable basicThrowable = new BasicThrowable(ex);
    return GSON.toJson(basicThrowable);
  }
}
