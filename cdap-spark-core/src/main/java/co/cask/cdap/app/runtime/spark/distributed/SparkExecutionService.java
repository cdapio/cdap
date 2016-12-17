/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.app.runtime.spark.distributed;

import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.app.runtime.spark.SparkMainWrapper;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.HttpExceptionHandler;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpHandler;
import co.cask.http.HttpResponder;
import co.cask.http.NettyHttpService;
import com.google.common.base.Charsets;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.gson.Gson;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.api.Command;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Type;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * The HTTP service for communicating with the {@link SparkMainWrapper} running in the driver
 * for controlling lifecycle management.
 */
public final class SparkExecutionService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(SparkExecutionService.class);
  private static final Gson GSON = new Gson();
  private static final Type TOKEN_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  // make sure this is less than twill's shutdown wait (30 seconds),
  // otherwise it causes really confusing behavior as this service will begin shutting down
  // and then twill may or may not kill it partway through.
  private static final long SHUTDOWN_WAIT_SECONDS = 20L;

  private final LocationFactory locationFactory;
  private final NettyHttpService httpServer;
  private final ProgramRunId programRunId;
  @Nullable
  private final WorkflowToken workflowToken;
  private final AtomicBoolean stopping;
  private final CountDownLatch stopLatch;

  public SparkExecutionService(LocationFactory locationFactory, String host,
                               ProgramRunId programRunId, @Nullable WorkflowToken workflowToken) {
    this.locationFactory = locationFactory;
    this.httpServer = NettyHttpService.builder()
      .addHttpHandlers(Collections.singletonList(new SparkControllerHandler()))
      .setHost(host)
      .setExceptionHandler(new HttpExceptionHandler())
      .build();
    this.stopping = new AtomicBoolean();
    this.stopLatch = new CountDownLatch(1);
    this.programRunId = programRunId;
    this.workflowToken = workflowToken;
  }

  /**
   * Returns the base {@link URI} for talking to this service remotely through HTTP.
   */
  public URI getBaseURI() {
    InetSocketAddress bindAddress = httpServer.getBindAddress();
    if (bindAddress == null) {
      throw new IllegalStateException("SparkExecutionService hasn't been started");
    }
    return URI.create(String.format("http://%s:%d", bindAddress.getHostName(), bindAddress.getPort()));
  }

  @Override
  protected void startUp() throws Exception {
    httpServer.startAndWait();
  }

  @Override
  protected void shutDown() throws Exception {
    stopping.set(true);
    if (!stopLatch.await(SHUTDOWN_WAIT_SECONDS, TimeUnit.SECONDS)) {
      LOG.warn("Timeout in waiting for Spark program to stop: {}", programRunId);
    }
    httpServer.stopAndWait();
  }

  public void shutdownNow() {
    stopLatch.countDown();
    stop();
  }

  /**
   * The {@link HttpHandler} for communicating with the Spark driver.
   */
  public final class SparkControllerHandler extends AbstractHttpHandler {

    /**
     * Handles heartbeat request from the running Spark program.
     */
    @POST
    @Path("/v1/spark/{programName}/runs/{runId}/heartbeat")
    public synchronized void heartbeat(HttpRequest request, HttpResponder responder,
                                       @PathParam("programName") String programName,
                                       @PathParam("runId") String runId) throws Exception {
      if (stopLatch.await(0, TimeUnit.SECONDS)) {
        throw new BadRequestException(
          String.format("Spark program '%s' is already stopped. Heartbeat is not accepted.", programRunId));
      }

      validateRequest(programName, runId);
      updateWorkflowToken(request.getContent());

      // If the stop was requested, send the "stop" command
      if (stopping.get()) {
        Command.Builder.of("stop");
        responder.sendJson(HttpResponseStatus.OK, SparkCommand.STOP);
      } else {
        responder.sendStatus(HttpResponseStatus.OK);
      }
    }

    /**
     * Handles execution completion request from the running Spark program.
     */
    @PUT
    @Path("/v1/spark/{programName}/runs/{runId}/completed")
    public synchronized void completed(HttpRequest request, HttpResponder responder,
                                       @PathParam("programName") String programName,
                                       @PathParam("runId") String runId) throws Exception {
      validateRequest(programName, runId);
      try {
        updateWorkflowToken(request.getContent());
        responder.sendStatus(HttpResponseStatus.OK);
      } finally {
        stopLatch.countDown();
      }
    }

    /**
     * Handles request for {@link Credentials}. This method will write the {@link Credentials} of the current user
     * to a location provided by the caller with permission only accessible by the current user. It expects the request
     * body is a JSON object with the target location URI to write to.
     */
    @POST
    @Path("/v1/spark/{programName}/runs/{runId}/credentials")
    public void writeCredentials(HttpRequest request, HttpResponder responder,
                                 @PathParam("programName") String programName,
                                 @PathParam("runId") String runId) throws Exception {
      CredentialsRequest credentialsRequest = GSON.fromJson(request.getContent().toString(Charsets.UTF_8),
                                                            CredentialsRequest.class);
      if (credentialsRequest == null || credentialsRequest.getUri() == null) {
        throw new BadRequestException("Expected request body a JSON object with an 'uri' field");
      }

      URI targetURI = credentialsRequest.getUri();
      URI homeURI = locationFactory.getHomeLocation().toURI();

      // The target URI scheme and authority must match with the one provided by the location factory
      if (!Objects.equals(homeURI.getScheme(), targetURI.getScheme())) {
        throw new BadRequestException("Target URI expected to have '" + homeURI.getScheme() + "' as the scheme");
      }
      if (!Objects.equals(targetURI.getAuthority(), homeURI.getAuthority())) {
        throw new BadRequestException("Target URI expected to have '" + homeURI.getAuthority() + "' as the authority");
      }

      Location targetLocation = locationFactory.create(targetURI);
      Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
      try (DataOutputStream os = new DataOutputStream(targetLocation.getOutputStream("600"))) {
        credentials.writeTokenStorageToStream(os);
      }

      LOG.debug("Credentials written for {} of run {} to {}: {}",
                programName, runId, targetURI, credentials.getAllTokens());

      responder.sendStatus(HttpResponseStatus.OK);
    }

    /**
     * Verifies the call is from the right client.
     */
    private void validateRequest(String programName, String runId) throws Exception {
      if (!programRunId.getProgram().equals(programName)) {
        throw new BadRequestException(
          String.format("Request program name '%s' is not the same as the context program name '%s",
                        programName, programRunId.getProgram())
        );
      }
      if (runId == null || !programRunId.getRun().equals(runId)) {
        throw new BadRequestException(
          String.format("Request runId '%s' is not the same as the context runId '%s'", runId, programRunId.getRun())
        );
      }
    }

    /**
     * Updates {@link WorkflowToken} of the program. It is a json Map<String, String> in the request body.
     */
    private void updateWorkflowToken(ChannelBuffer requestBody) {
      if (!requestBody.readable()) {
        return;
      }
      if (workflowToken == null) {
        // This shouldn't happen. Just log and ignore the update
        LOG.warn("Spark program is not running inside Workflow. Ignore workflow token update: {}", programRunId);
        return;
      }

      try (Reader reader = new InputStreamReader(new ChannelBufferInputStream(requestBody), Charsets.UTF_8)) {
        Map<String, String> token = GSON.fromJson(reader, TOKEN_TYPE);
        for (Map.Entry<String, String> entry : token.entrySet()) {
          workflowToken.put(entry.getKey(), entry.getValue());
        }
      } catch (IOException e) {
        // Shouldn't happen, since all reading is from in-memory buffer
        LOG.warn("Exception when deocoding workflow token update request for {}", programRunId, e);
      }
    }
  }

}
