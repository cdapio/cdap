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
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.common.http.HttpMethod;
import org.apache.avro.Schema;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * The client for talking to the {@link RuntimeServer}.
 */
public class RuntimeClient {

  static final int CHUNK_SIZE = 1 << 15;  // 32K

  private final RemoteClient remoteClient;

  @Inject
  RuntimeClient(DiscoveryServiceClient discoveryClient) {
    this.remoteClient = new RemoteClient(discoveryClient, Constants.Service.RUNTIME,
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
    if (!NamespaceId.SYSTEM.equals(topicId.getNamespaceId())) {
      throw new IllegalArgumentException("Only topic in the system namespace is supported");
    }

    String path = String.format("%s/apps/%s/versions/%s/%s/%s/runs/%s/topics/%s",
                                programRunId.getNamespace(),
                                programRunId.getApplication(),
                                programRunId.getVersion(),
                                programRunId.getType().getCategoryName(),
                                programRunId.getProgram(),
                                programRunId.getRun(),
                                topicId.getTopic());

    // Stream out the messages
    HttpURLConnection urlConn = remoteClient.openConnection(HttpMethod.POST, path);
    try {
      urlConn.setChunkedStreamingMode(CHUNK_SIZE);
      urlConn.setRequestProperty(HttpHeaders.CONTENT_TYPE, "avro/binary");

      try (OutputStream os = urlConn.getOutputStream()) {
        writeMessages(messages, EncoderFactory.get().directBinaryEncoder(os, null));
      }

      throwIfError(programRunId, topicId, urlConn);

      // Discard everything from the inputstream. This is needed to allow connection reuse.
      try (InputStream is = urlConn.getInputStream()) {
        if (is != null) {
          ByteStreams.toByteArray(is);
        }
      }
    } finally {
      urlConn.disconnect();
    }
  }

  /**
   * Validates the responds from the given {@link HttpURLConnection} to be 200, or throws exception if it is not 200.
   */
  private void throwIfError(ProgramRunId programRunId,
                            TopicId topicId, HttpURLConnection urlConn) throws IOException, BadRequestException {
    int responseCode = urlConn.getResponseCode();
    if (responseCode == HttpURLConnection.HTTP_OK) {
      return;
    }
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

      throw new IOException("Failed to send message for program run " + programRunId + " to topic " + topicId
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
  private void writeMessages(Iterator<Message> messages, Encoder encoder) throws IOException {
    encoder.writeArrayStart();

    // Buffer payloads to the size of one HTTP chunk, then write out one array block.
    // See the AVRO spec https://avro.apache.org/docs/current/spec.html#Data+Serialization+and+Deserialization
    // for details of how an array is encoded into multiple array blocks
    List<byte[]> payloads = new ArrayList<>();
    long blockSize = 0;
    while (messages.hasNext()) {
      byte[] payload = messages.next().getPayload();
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
