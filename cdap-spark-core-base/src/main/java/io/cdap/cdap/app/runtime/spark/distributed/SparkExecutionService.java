/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.app.runtime.spark.distributed;

import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.gson.Gson;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.app.runtime.spark.SparkMainWrapper;
import io.cdap.cdap.app.runtime.spark.SparkPackageUtils;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.HttpExceptionHandler;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpHandler;
import io.cdap.http.HttpResponder;
import io.cdap.http.NettyHttpService;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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
  private final AtomicLong shutdownWaitSeconds;
  private final CountDownLatch stopLatch;
  private volatile SparkCommand stopCommand;

  public SparkExecutionService(LocationFactory locationFactory, String host,
                               ProgramRunId programRunId, @Nullable WorkflowToken workflowToken) {
    this.locationFactory = locationFactory;
    this.httpServer = NettyHttpService.builder(programRunId.getProgram() + "-spark-exec-service")
      .setHttpHandlers(Collections.singletonList(new SparkControllerHandler()))
      .setHost(host)
      .setExceptionHandler(new HttpExceptionHandler())
      .build();
    this.stopLatch = new CountDownLatch(1);
    this.programRunId = programRunId;
    this.workflowToken = workflowToken;
    this.shutdownWaitSeconds = new AtomicLong(SHUTDOWN_WAIT_SECONDS);
  }

  /**
   * Returns the base {@link URI} for talking to this service remotely through HTTP.
   */
  public URI getBaseURI() {
    InetSocketAddress bindAddress = getBindAddress();
    return URI.create(String.format("http://%s:%d", bindAddress.getHostName(), bindAddress.getPort()));
  }

  /**
   * Returns the socket address that the service is bound to.
   */
  public InetSocketAddress getBindAddress() {
    InetSocketAddress bindAddress = httpServer.getBindAddress();
    if (bindAddress == null) {
      throw new IllegalStateException("SparkExecutionService hasn't been started");
    }
    return bindAddress;
  }

  /**
   * Sets the number of seconds to wait for the Spark job to complete during shutdown.
   */
  public void setShutdownWaitSeconds(long seconds) {
    if (seconds < 0L) {
      throw new IllegalStateException("Shutdown wait seconds must be >= 0");
    }
    shutdownWaitSeconds.set(seconds);
  }

  @Override
  protected void startUp() throws Exception {
    httpServer.start();
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Shutting down SparkExecutionService");
    long waitSeconds = shutdownWaitSeconds.get();
    long currentTs = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    long terminateTs = (Long.MAX_VALUE - currentTs < waitSeconds) ? Long.MAX_VALUE : currentTs + waitSeconds;

    stopCommand = SparkCommand.createStop(terminateTs);

    if (!stopLatch.await(waitSeconds, TimeUnit.SECONDS)) {
      LOG.warn("Timeout in waiting for Spark program to stop: {}", programRunId);
    }
    LOG.info("Shutting down HTTP server");
    httpServer.stop();
  }

  /**
   * Shutdown this service without waiting for the `completed` call from the {@link SparkDriverService}.
   * This method is used when the Spark job is terminated as reported by SparkSubmit.
   * If the Spark job was terminated normally, the `completed` should been called from the driver service already
   * prior to this method being called.
   */
  public void shutdownNow() {
    stopLatch.countDown();
    stopAndWait();
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
    public synchronized void heartbeat(FullHttpRequest request, HttpResponder responder,
                                       @PathParam("programName") String programName,
                                       @PathParam("runId") String runId) throws Exception {
      if (stopLatch.await(0, TimeUnit.SECONDS)) {
        throw new BadRequestException(
          String.format("Spark program '%s' is already stopped. Heartbeat is not accepted.", programRunId));
      }

      validateRequest(programName, runId);
      updateWorkflowToken(request.content());

      // If the stop command is present, send the stop command to request the spark job to stop
      if (stopCommand != null) {
        responder.sendJson(HttpResponseStatus.OK, GSON.toJson(stopCommand));
      } else {
        responder.sendStatus(HttpResponseStatus.OK);
      }
    }

    /**
     * Handles execution completion request from the running Spark program.
     */
    @PUT
    @Path("/v1/spark/{programName}/runs/{runId}/completed")
    public synchronized void completed(FullHttpRequest request, HttpResponder responder,
                                       @PathParam("programName") String programName,
                                       @PathParam("runId") String runId) throws Exception {
      LOG.info("Spark program completed {} {}", programName, runId);
      validateRequest(programName, runId);
      try {
        updateWorkflowToken(request.content());
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
    public void writeCredentials(FullHttpRequest request, HttpResponder responder,
                                 @PathParam("programName") String programName,
                                 @PathParam("runId") String runId) throws Exception {
      CredentialsRequest credentialsRequest = GSON.fromJson(request.content().toString(StandardCharsets.UTF_8),
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
      Credentials creds = removeSecretKeys(new Credentials(UserGroupInformation.getCurrentUser().getCredentials()));
      try (DataOutputStream os = new DataOutputStream(targetLocation.getOutputStream("600"))) {
        creds.writeTokenStorageToStream(os);
      }

      LOG.debug("Credentials written for {} of run {} to {}: {}", programName, runId, targetURI, creds.getAllTokens());

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
      if (!programRunId.getRun().equals(runId)) {
        throw new BadRequestException(
          String.format("Request runId '%s' is not the same as the context runId '%s'", runId, programRunId.getRun())
        );
      }
    }

    /**
     * Updates {@link WorkflowToken} of the program. It is a json Map<String, String> in the request body.
     */
    private void updateWorkflowToken(ByteBuf requestBody) {
      if (!requestBody.isReadable()) {
        return;
      }
      if (workflowToken == null) {
        // This shouldn't happen. Just log and ignore the update
        LOG.warn("Spark program is not running inside Workflow. Ignore workflow token update: {}", programRunId);
        return;
      }

      try (Reader reader = new InputStreamReader(new ByteBufInputStream(requestBody), StandardCharsets.UTF_8)) {
        GSON.<Map<String, String>>fromJson(reader, TOKEN_TYPE).forEach(workflowToken::put);
      } catch (IOException e) {
        // Shouldn't happen, since all reading is from in-memory buffer
        LOG.warn("Exception when deocoding workflow token update request for {}", programRunId, e);
      }
    }

    /**
     * Removes secret keys from the given {@link Credentials} and retains tokens only.
     * This is needed for CDAP-12752.
     */
    private Credentials removeSecretKeys(Credentials credentials) {
      // In Hadoop 2.6+, there are APIs to do so. If it is available, use them.
      // For 2.6+, effectively we want to do:
      // for (Text name : credentials.getAllSecretKeys()) {
      //   crentials.removeSecretKey(name);
      // }
      try {
        List<Text> names = (List<Text>) credentials.getClass().getMethod("getAllSecretKeys").invoke(credentials);
        Method removeSecretKey = credentials.getClass().getMethod("removeSecretKey", Text.class);
        for (Text name : names) {
          removeSecretKey.invoke(credentials, name);
        }
      } catch (Exception e) {
        try {
          // If there is any exception from above, try to use reflection to access the secretKeys map field
          // directly and clear it.
          Field secretKeysMap = credentials.getClass().getDeclaredField("secretKeysMap");
          secretKeysMap.setAccessible(true);
          ((Map<Text, byte[]>) secretKeysMap.get(credentials)).clear();
        } catch (Exception ex) {
          // This shouldn't happen. If it really does (e.g. for future hadoop API changes), log a warning
          if (credentials.getSecretKey(new Text("sparkCookie")) != null) {
            Properties defaultConf = SparkPackageUtils.getSparkDefaultConf();
            boolean authEnabled = Boolean.parseBoolean(defaultConf.getProperty("spark.authenticate"));
            boolean shuffleEnabled = Boolean.parseBoolean(defaultConf.getProperty("spark.shuffle.service.enabled"));

            if (authEnabled && shuffleEnabled) {
              LOG.warn("Unable to remove 'sparkCookie' from UGI credentials. The Spark program might fail.");
            }
          }
        }
      }

      return credentials;
    }
  }
}
