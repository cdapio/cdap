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

import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.internal.app.runtime.workflow.BasicWorkflowToken;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.common.base.Charsets;
import com.google.common.base.Functions;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.hadoop.security.Credentials;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A client to the {@link SparkExecutionService}.
 */
public class SparkExecutionClient {

  private static final Gson GSON = new Gson();
  private static final Type TOKEN_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  private final URI executionServiceBaseURI;
  private final ProgramRunId programRunId;

  public SparkExecutionClient(URI executionServiceBaseURI, ProgramRunId programRunId) {
    this.executionServiceBaseURI = executionServiceBaseURI;
    this.programRunId = programRunId;
  }

  /**
   * Sends a heartbeat request to the execution service.
   *
   * @param workflowToken the {@link BasicWorkflowToken} to send in the heartbeat request if it is not {@code null}.
   *                      only changes made by this Spark program will be sent.
   * @return the {@link SparkCommand} returned by the service or {@code null} if there is no command.
   */
  @Nullable
  public SparkCommand heartbeat(@Nullable BasicWorkflowToken workflowToken) throws Exception {
    HttpURLConnection urlConn = openConnection("heartbeat");
    try {
      urlConn.setRequestMethod("POST");
      writeWorkflowToken(workflowToken, urlConn);
      validateResponse(urlConn);

      try (Reader reader = new InputStreamReader(urlConn.getInputStream(), Charsets.UTF_8)) {
        return GSON.fromJson(reader, SparkCommand.class);
      }

    } finally {
      urlConn.disconnect();
    }
  }

  /**
   * Sends a execution completed request to the execution service.
   *
   * @param workflowToken the {@link BasicWorkflowToken} to send in the heartbeat request if it is not {@code null}.
   *                      only changes made by this Spark program will be sent.
   */
  public void completed(@Nullable BasicWorkflowToken workflowToken) throws Exception {
    HttpURLConnection urlConn = openConnection("completed");
    try {
      urlConn.setRequestMethod("PUT");
      writeWorkflowToken(workflowToken, urlConn);
      validateResponse(urlConn);
    } finally {
      urlConn.disconnect();
    }
  }

  /**
   * Sends a write credentials request to the execution service.
   *
   * @param location the location where the credentials will write to.
   */
  public void writeCredentials(Location location) throws Exception {
    HttpURLConnection urlConn = openConnection("credentials");
    try {
      urlConn.setRequestMethod("POST");
      try (Writer writer = new OutputStreamWriter(urlConn.getOutputStream(), Charsets.UTF_8)) {
        GSON.toJson(new CredentialsRequest(location.toURI()), writer);
      }
      validateResponse(urlConn);
    } finally {
      urlConn.disconnect();
    }
  }

  /**
   * Opens a {@link HttpURLConnection} to the execution service endpoint for the given action.
   */
  private HttpURLConnection openConnection(String action) throws IOException {
    String programName = programRunId.getProgram();
    String runId = programRunId.getRun();
    URL url = executionServiceBaseURI.resolve(
      String.format("/v1/spark/%s/runs/%s/%s", programName, runId, action)).toURL();
    HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
    urlConn.setDoOutput(true);
    return urlConn;
  }

  /**
   * Writes the workflow tokens modified by this program to the given URL connection.
   */
  private void writeWorkflowToken(@Nullable BasicWorkflowToken workflowToken,
                                  HttpURLConnection urlConn) throws IOException {
    try (Writer writer = new OutputStreamWriter(urlConn.getOutputStream(), Charsets.UTF_8)) {
      if (workflowToken != null) {
        GSON.toJson(Maps.transformValues(workflowToken.getAllFromCurrentNode(),
                                         Functions.toStringFunction()), TOKEN_TYPE, writer);
      }
    }
  }

  private void validateResponse(HttpURLConnection urlConn) throws Exception {
    int responseCode = urlConn.getResponseCode();
    if (responseCode == HttpURLConnection.HTTP_OK) {
      return;
    }
    String errorMessage = new String(ByteStreams.toByteArray(urlConn.getErrorStream()), Charsets.UTF_8);
    if (responseCode == HttpURLConnection.HTTP_BAD_REQUEST) {
      throw new BadRequestException(errorMessage);
    }
    throw new Exception("Spark execution service request failed: " + errorMessage);
  }
}
