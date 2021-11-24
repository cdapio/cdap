/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.monitor;

import com.google.common.io.ByteStreams;
import com.google.common.net.HttpHeaders;
import com.google.inject.Inject;
import io.cdap.cdap.api.messaging.Message;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.ServiceUnavailableException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.common.http.HttpMethod;
import org.apache.avro.Schema;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.zip.GZIPOutputStream;
import javax.ws.rs.core.MediaType;

/**
 * The client for talking to the {@link RuntimeServer}.
 */
public class RuntimeClient {

  private static final Logger LOG = LoggerFactory.getLogger(RuntimeClient.class);
  static final int CHUNK_SIZE = 1 << 15;  // 32K

  private final boolean compression;
  private final RemoteClient remoteClient;

  @Inject
  public RuntimeClient(CConfiguration cConf, RemoteClientFactory remoteClientFactory) {
    this.compression = cConf.getBoolean(Constants.RuntimeMonitor.COMPRESSION_ENABLED);
    this.remoteClient = remoteClientFactory.createRemoteClient(
      Constants.Service.RUNTIME,
      new DefaultHttpRequestConfig(false),
      Constants.Gateway.INTERNAL_API_VERSION_3 + "/runtime/namespaces/");

    // Validate the schema is what as expected by the logic of this client.
    // This is to make sure unit test will fail if schema is changed without changing the logic in this class.
    Schema schema = MonitorSchemas.V2.MonitorRequest.SCHEMA;
    if (schema == null) {
      throw new IllegalStateException("Missing MonitorRequest schema");
    }
    if (schema.getType() != Schema.Type.ARRAY || schema.getElementType().getType() != Schema.Type.BYTES) {
      throw new IllegalStateException("MonitorRequest schema should be an array of bytes");
    }
  }

  /**
   * Sends messages to the given TMS system topic from the given program run.
   *
   * @param programRunId the program run id of the program run
   * @param topicId the topic to write to. The namespace must be {@link NamespaceId#SYSTEM}
   * @param messages the list of messages to send
   * @throws IOException if failed to send all the given messages
   * @throws BadRequestException if the server denial the request due to bad request
   * @throws ServiceUnavailableException if the server is not available
   */
  public void sendMessages(ProgramRunId programRunId,
                           TopicId topicId, Iterator<Message> messages) throws IOException, BadRequestException {
    LOG.info("Trying to send messages");
    if (!NamespaceId.SYSTEM.equals(topicId.getNamespaceId())) {
      LOG.error("Namespace id does not exist");
      throw new IllegalArgumentException("Only topic in the system namespace is supported");
    }
    LOG.info("Creating path now");
    String path = String.format("%s/apps/%s/versions/%s/%s/%s/runs/%s/topics/%s",
                                programRunId.getNamespace(),
                                programRunId.getApplication(),
                                programRunId.getVersion(),
                                programRunId.getType().getCategoryName(),
                                programRunId.getProgram(),
                                programRunId.getRun(),
                                topicId.getTopic());
    LOG.info("Opening http url conn to path - {}", path);
    // Stream out the messages
    HttpURLConnection urlConn = remoteClient.openConnection(HttpMethod.POST, path);
    LOG.info("Making a call to url - {}", urlConn.getURL());
    try {
      urlConn.setChunkedStreamingMode(CHUNK_SIZE);
      LOG.info("Set chunked streaming mode");
      urlConn.setRequestProperty(HttpHeaders.CONTENT_TYPE, "avro/binary");
      LOG.info("Set request property");
      try (OutputStream os = openOutputStream(urlConn)) {
        LOG.info("Opened output stream");
        writeMessages(messages, EncoderFactory.get().directBinaryEncoder(os, null), topicId.getTopic());
        LOG.info("Wrote messages");
      }
      LOG.info("Will throw error if any");
      throwIfError(programRunId, urlConn);
      // return grace period
      LOG.info("No error was thrown");
      LOG.info("Response code is {}", urlConn.getResponseCode());
    } finally {
      LOG.info("Will close connection now");
      closeURLConnection(urlConn);
    }
  }

  /**
   * This method should call the runtime handler method to check if it should shutdown gracefuly
   * @return
   */
  public long stoppingTimeoutForProgram(ProgramRunId programRunId, TopicId topicId)
    throws IOException, BadRequestException {
    LOG.info("Checking if program should be stopped");
    LOG.info("program ID is {}", programRunId);
    String path = String.format("%s/apps/%s/versions/%s/%s/%s/runs/%s/topics/%s",
                                programRunId.getNamespace(),
                                programRunId.getApplication(),
                                programRunId.getVersion(),
                                programRunId.getType().getCategoryName(),
                                programRunId.getProgram(),
                                programRunId.getRun(),
                                topicId.getTopic());
    LOG.info("Making a post call to path - {}", path);
    // Get if shutdown should be graceful
    // based on the timeout value trigger a stop by calling dataprocRuntimeJobManager.stop()
    HttpURLConnection urlConn = remoteClient.openConnection(HttpMethod.POST, path);
    try {
      LOG.info("Full url - {}", urlConn.getURL());
      urlConn.setChunkedStreamingMode(CHUNK_SIZE);
      urlConn.setRequestProperty(HttpHeaders.CONTENT_TYPE, "avro/binary");
      LOG.info("OPening output stream");
      try (OutputStream os = openOutputStream(urlConn)) {
        LOG.info("writing empty message into OS");
        writeMessages(Collections.emptyIterator(), EncoderFactory.get().directBinaryEncoder(os, null),
                      topicId.getTopic());
      }
      LOG.info("Will throw any error");
      throwIfError(programRunId, urlConn);
      LOG.info("No error, fetching response code now");
      int responseCode = urlConn.getResponseCode();
      LOG.info("Response code is {}", responseCode);
      long stoppingTimeout = Long.valueOf(urlConn.getResponseMessage());
      LOG.info("Stopping timeout is {}", stoppingTimeout);
      return stoppingTimeout;
    } catch (Throwable e) {
      LOG.error("An exception occurred - {}", e.getMessage());
      throw e;
    } finally {
      closeURLConnection(urlConn);
    }
  }

  /**
   * Uploads Spark program event logs to the runtime service.
   *
   * @param programRunId the program run id of the program run
   * @param eventFile the local file containing the event logs
   * @throws IOException if failed to send the event logs
   * @throws ServiceUnavailableException if the service is not available
   */
  public void uploadSparkEventLogs(ProgramRunId programRunId, File eventFile) throws IOException {
    String path = String.format("%s/apps/%s/versions/%s/%s/%s/runs/%s/spark-event-logs/%s",
                                programRunId.getNamespace(),
                                programRunId.getApplication(),
                                programRunId.getVersion(),
                                programRunId.getType().getCategoryName(),
                                programRunId.getProgram(),
                                programRunId.getRun(),
                                eventFile.getName());

    // Stream out the messages
    HttpURLConnection urlConn = remoteClient.openConnection(HttpMethod.POST, path);
    LOG.info("Making a call to url - {}", urlConn.getURL());
    try {
      urlConn.setChunkedStreamingMode(CHUNK_SIZE);
      urlConn.setRequestProperty(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_OCTET_STREAM);

      // No need to use compression since event files should have compression enabled
      try (OutputStream os = urlConn.getOutputStream()) {
        Files.copy(eventFile.toPath(), os);
        throwIfError(programRunId, urlConn);
      } catch (BadRequestException e) {
        // Just treat bad request as IOException since it won't be retriable
        throw new IOException(e);
      }
    } finally {
      closeURLConnection(urlConn);
    }
  }

  /**
   * Opens a {@link OutputStream} to the given {@link URLConnection}. If {@link #compression} is {@code true},
   * the output stream will be wrapped with a {@link GZIPOutputStream} with appropriate request header set.
   */
  private OutputStream openOutputStream(URLConnection urlConn) throws IOException {
    if (!compression) {
      return urlConn.getOutputStream();
    }
    urlConn.setRequestProperty(HttpHeaders.CONTENT_ENCODING, "gzip");
    return new GZIPOutputStream(urlConn.getOutputStream());
  }

  /**
   * Closes the given {@link URLConnection} so that the underlying connection can be reused.
   *
   * @param urlConn the URL connection to close
   */
  private void closeURLConnection(HttpURLConnection urlConn) {
    try (InputStream is = urlConn.getInputStream()) {
      if (is != null) {
        ByteStreams.toByteArray(is);
      }
    } catch (IOException e) {
      // No need to throw. When the URLConnection.disconnect() is called, it will close the socket if the
      // input stream is not in a reusable state.
    } finally {
      urlConn.disconnect();
    }
  }

  /**
   * Validates the responds from the given {@link HttpURLConnection} to be 200, or throws exception if it is not 200.
   */
  private void throwIfError(ProgramRunId programRunId,
                            HttpURLConnection urlConn) throws IOException, BadRequestException {
    LOG.info("Inside throw if error");
    int responseCode = urlConn.getResponseCode();
    LOG.info("Got response code - {}", responseCode);
    if (responseCode == HttpURLConnection.HTTP_OK) {
      LOG.info("Response code is http ok");
      return;
    }
    LOG.info("Response code is not http ok, will analyze the error now");
    try (InputStream errorStream = urlConn.getErrorStream()) {
      String errorMsg = "unknown error";
      if (errorStream != null) {
        errorMsg = new String(ByteStreams.toByteArray(errorStream), StandardCharsets.UTF_8);
      }
      switch (responseCode) {
        case HttpURLConnection.HTTP_BAD_REQUEST:
          throw new BadRequestException(errorMsg);
        case HttpURLConnection.HTTP_UNAVAILABLE:
          throw new ServiceUnavailableException(Constants.Service.RUNTIME, errorMsg);
      }

      throw new IOException("Failed to send message for program run " + programRunId + " to " + urlConn.getURL()
                              + ". Respond code: " + responseCode + ". Error: " + errorMsg);
    }
  }

  /**
   * Returns the size in bytes of the avro encoded byte array for the given byte array.
   */
  private int encodedLength(byte[] bytes) {
    int len = bytes.length;
    int size = len;
    while (len != 0) {
      size++;
      len >>= 6;
    }
    return size;
  }

  /**
   * Streaming encode the given list of messages based on the schema
   * as defined by the {@link MonitorSchemas.V2.MonitorRequest}.
   */
  private void writeMessages(Iterator<Message> messages, Encoder encoder, String topic) throws IOException {
    encoder.writeArrayStart();

    // Buffer payloads to the size of one HTTP chunk, then write out one array block.
    // See the AVRO spec https://avro.apache.org/docs/current/spec.html#Data+Serialization+and+Deserialization
    // for details of how an array is encoded into multiple array blocks
    List<byte[]> payloads = new ArrayList<>();
    long blockSize = 0;
    while (messages.hasNext()) {
      Message message = messages.next();
      byte[] payload = message.getPayload();
      if (topic.equalsIgnoreCase("programstatusevent")) {
        LOG.info("Message payload as string {}", message.getPayloadAsString());
      }
      payloads.add(payload);
      blockSize += encodedLength(payload);
      if (blockSize >= CHUNK_SIZE) {
        writePayloads(payloads, encoder);
        payloads.clear();
        blockSize = 0;
        encoder.flush();
      }
    }
    if (!payloads.isEmpty()) {
      writePayloads(payloads, encoder);
    }
    encoder.writeArrayEnd();
  }

  /**
   * Encodes and writes all the payloads as one avro array block.
   */
  private void writePayloads(List<byte[]> payloads, Encoder encoder) throws IOException {
    encoder.setItemCount(payloads.size());
    for (byte[] payload : payloads) {
      encoder.startItem();
      encoder.writeBytes(payload);
    }
  }
}
