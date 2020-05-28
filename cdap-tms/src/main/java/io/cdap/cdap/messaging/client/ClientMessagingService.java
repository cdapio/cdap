/*
 * Copyright © 2016-2018 Cask Data, Inc.
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

package io.cdap.cdap.messaging.client;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.net.HttpHeaders;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.AbstractCloseableIterator;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.messaging.TopicAlreadyExistsException;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.common.ServiceUnavailableException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.messaging.MessageFetcher;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.RollbackDetail;
import io.cdap.cdap.messaging.Schemas;
import io.cdap.cdap.messaging.StoreRequest;
import io.cdap.cdap.messaging.TopicMetadata;
import io.cdap.cdap.messaging.data.RawMessage;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequestConfig;
import io.cdap.common.http.HttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.tephra.TransactionCodec;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import java.util.zip.DeflaterInputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import javax.annotation.Nullable;

/**
 * The client implementation of {@link MessagingService}. This client is intended for internal
 * higher level API implementation only.
 *
 * NOTE: This class shouldn't expose to end user (e.g. cdap-client module).
 */
public final class ClientMessagingService implements MessagingService {

  private static final HttpRequestConfig HTTP_REQUEST_CONFIG = new DefaultHttpRequestConfig(false);
  private static final TransactionCodec TRANSACTION_CODEC = new TransactionCodec();
  private static final Gson GSON = new Gson();
  // These types for only for Gson to use, hence using the gson TypeToken instead of guava one
  private static final Type TOPIC_PROPERTY_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Type TOPIC_LIST_TYPE = new TypeToken<List<String>>() { }.getType();

  private final RemoteClient remoteClient;
  private final boolean compressPayload;

  @Inject
  ClientMessagingService(CConfiguration cConf, DiscoveryServiceClient discoveryServiceClient) {
    this(discoveryServiceClient, cConf.getBoolean(Constants.MessagingSystem.HTTP_COMPRESS_PAYLOAD));
  }

  @VisibleForTesting
  public ClientMessagingService(DiscoveryServiceClient discoveryServiceClient, boolean compressPayload) {
    this.remoteClient = new RemoteClient(discoveryServiceClient, Constants.Service.MESSAGING_SERVICE,
                                         HTTP_REQUEST_CONFIG, "/v1/namespaces/");
    this.compressPayload = compressPayload;
  }

  @Override
  public void createTopic(TopicMetadata topicMetadata) throws TopicAlreadyExistsException, IOException {
    TopicId topicId = topicMetadata.getTopicId();

    HttpRequest request = remoteClient.requestBuilder(HttpMethod.PUT, createTopicPath(topicId))
      .withBody(GSON.toJson(topicMetadata.getProperties()))
      .build();
    HttpResponse response = remoteClient.execute(request);

    if (response.getResponseCode() == HttpURLConnection.HTTP_CONFLICT) {
      throw new TopicAlreadyExistsException(topicId.getNamespace(), topicId.getTopic());
    }
    handleError(response, "Failed to create topic " + topicId);
  }

  @Override
  public void updateTopic(TopicMetadata topicMetadata) throws TopicNotFoundException, IOException {
    TopicId topicId = topicMetadata.getTopicId();

    HttpRequest request = remoteClient.requestBuilder(HttpMethod.PUT, createTopicPath(topicId) + "/properties")
      .withBody(GSON.toJson(topicMetadata.getProperties()))
      .build();
    HttpResponse response = remoteClient.execute(request);

    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new TopicNotFoundException(topicId.getNamespace(), topicId.getTopic());
    }
    handleError(response, "Failed to update topic " + topicId);
  }

  @Override
  public void deleteTopic(TopicId topicId) throws TopicNotFoundException, IOException {
    HttpRequest request = remoteClient.requestBuilder(HttpMethod.DELETE, createTopicPath(topicId)).build();
    HttpResponse response = remoteClient.execute(request);

    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new TopicNotFoundException(topicId.getNamespace(), topicId.getTopic());
    }
    handleError(response, "Failed to update topic " + topicId);
  }

  @Override
  public TopicMetadata getTopic(TopicId topicId) throws TopicNotFoundException, IOException {
    HttpRequest request = remoteClient.requestBuilder(HttpMethod.GET, createTopicPath(topicId)).build();
    HttpResponse response = remoteClient.execute(request);

    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new TopicNotFoundException(topicId.getNamespace(), topicId.getTopic());
    }
    handleError(response, "Failed to update topic " + topicId);

    Map<String, String> properties = GSON.fromJson(response.getResponseBodyAsString(), TOPIC_PROPERTY_TYPE);
    return new TopicMetadata(topicId, properties);
  }

  @Override
  public List<TopicId> listTopics(NamespaceId namespaceId) throws IOException {
    HttpRequest request = remoteClient.requestBuilder(HttpMethod.GET, namespaceId.getNamespace() + "/topics").build();
    HttpResponse response = remoteClient.execute(request);

    handleError(response, "Failed to list topics in namespace " + namespaceId);
    List<String> topics = GSON.fromJson(response.getResponseBodyAsString(), TOPIC_LIST_TYPE);
    List<TopicId> result = new ArrayList<>(topics.size());
    for (String topic : topics) {
      result.add(namespaceId.topic(topic));
    }

    return Collections.unmodifiableList(result);
  }

  @Override
  public MessageFetcher prepareFetch(TopicId topicId) {
    return new ClientMessageFetcher(topicId);
  }

  @Nullable
  @Override
  public RollbackDetail publish(StoreRequest request) throws TopicNotFoundException, IOException {
    HttpResponse response = performWriteRequest(request, true);

    byte[] body = response.getResponseBody();
    if (body.length == 0) {
      return null;
    }

    // It has rollback detail, verify the content-type and decode it
    verifyContentType(response.getHeaders().asMap(), "avro/binary");
    return new ClientRollbackDetail(body);
  }

  @Override
  public void storePayload(StoreRequest request) throws TopicNotFoundException, IOException {
    performWriteRequest(request, false);
  }

  @Override
  public void rollback(TopicId topicId, RollbackDetail rollbackDetail) throws TopicNotFoundException, IOException {
    ByteBuffer requestBody = (rollbackDetail instanceof ClientRollbackDetail)
      ? ByteBuffer.wrap(((ClientRollbackDetail) rollbackDetail).getEncoded())
      : encodeRollbackDetail(rollbackDetail);

    HttpRequest httpRequest = remoteClient.requestBuilder(HttpMethod.POST, createTopicPath(topicId) + "/rollback")
      .addHeader(HttpHeaders.CONTENT_TYPE, "avro/binary")
      .withBody(requestBody)
      .build();

    HttpResponse response = remoteClient.execute(httpRequest);

    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new TopicNotFoundException(topicId.getNamespace(), topicId.getTopic());
    }
    handleError(response, "Failed to rollback message in topic " + topicId
                                      + " with rollback detail " + rollbackDetail);
  }

  /**
   * Makes a request to the server for writing to the messaging system
   *
   * @param request contains information about what to write
   * @param publish {@code true} to make publish call, {@code false} to make store call.
   * @return the response from the server
   * @throws IOException if failed to perform the write operation
   * @throws TopicNotFoundException if the topic to write to does not exist
   */
  private HttpResponse performWriteRequest(StoreRequest request,
                                           boolean publish) throws IOException, TopicNotFoundException {
    GenericRecord record = new GenericData.Record(Schemas.V1.PublishRequest.SCHEMA);
    if (request.isTransactional()) {
      record.put("transactionWritePointer", request.getTransactionWritePointer());
    }
    record.put("messages", convertPayloads(request));

    // Encode the request as avro
    ExposedByteArrayOutputStream os = new ExposedByteArrayOutputStream();
    try (OutputStream encoderOutput = compressOutputStream(os)) {
      Encoder encoder = EncoderFactory.get().directBinaryEncoder(encoderOutput, null);

      DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(Schemas.V1.PublishRequest.SCHEMA);
      datumWriter.write(record, encoder);
      encoder.flush();
    }

    // Make the publish request
    String writeType = publish ? "publish" : "store";
    TopicId topicId = request.getTopicId();
    Map<String, String> headers = new HashMap<>();
    headers.put(HttpHeaders.CONTENT_TYPE, "avro/binary");
    if (compressPayload) {
      headers.put(HttpHeaders.CONTENT_ENCODING, "gzip");
    }

    HttpRequest httpRequest = remoteClient.requestBuilder(HttpMethod.POST, createTopicPath(topicId) + "/" + writeType)
      .addHeaders(headers)
      .withBody(os.toByteBuffer())
      .build();

    HttpResponse response = remoteClient.execute(httpRequest);

    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new TopicNotFoundException(topicId.getNamespace(), topicId.getTopic());
    }
    handleError(response, "Failed to " + writeType + " message to topic " + topicId);
    return response;
  }

  /**
   * Wraps the given output stream with {@link GZIPOutputStream} if payload compression is enabled.
   */
  private OutputStream compressOutputStream(OutputStream outputStream) throws IOException {
    return compressPayload ? new GZIPOutputStream(outputStream) : outputStream;
  }

  /**
   * Creates the URL path for making HTTP requests for the given topic.
   */
  private String createTopicPath(TopicId topicId) {
    return topicId.getNamespace() + "/topics/" + topicId.getTopic();
  }

  /**
   * Handles error response from the given {@link HttpResponse}.
   */
  private void handleError(final HttpResponse response, String errorPrefix) throws IOException {
    handleError(response.getResponseCode(), response::toString, errorPrefix);
  }

  /**
   * Handles error response based on the given response code.
   */
  private void handleError(int responseCode, Supplier<String> responseBodySupplier,
                           String errorPrefix) throws IOException {
    switch (responseCode) {
      case HttpURLConnection.HTTP_OK:
        return;
      case HttpURLConnection.HTTP_BAD_REQUEST:
        throw new IllegalArgumentException(errorPrefix + ". Reason: " + responseBodySupplier.get());
      case HttpURLConnection.HTTP_UNAVAILABLE:
        throw new ServiceUnavailableException(Constants.Service.MESSAGING_SERVICE);
      default:
        throw new IOException(errorPrefix + ". Reason: " + responseBodySupplier.get());
    }
  }

  /**
   * Converts the payloads carried by the given {@link StoreRequest} into a {@link List} of {@link ByteBuffer},
   * which is needed by the avro record.
   */
  private List<ByteBuffer> convertPayloads(StoreRequest request) {
    return StreamSupport.stream(request.spliterator(), false).map(ByteBuffer::wrap).collect(Collectors.toList());
  }

  /**
   * Encodes the given {@link RollbackDetail} as expected by the rollback call. This method is rarely used
   * as the call to {@link #rollback(TopicId, RollbackDetail)} expects a {@link ClientRollbackDetail} which
   * already contains the encoded bytes.
   *
   * This method looks very similar to the {@code StoreHandler.encodeRollbackDetail} method, but is intended to have
   * them separated. This is to allow client side classes be moved to separate module without any dependency
   * on the server side (this can also be done with a util method in a common module, but it is kind of overkill
   * for a simple method like this for now).
   */
  private ByteBuffer encodeRollbackDetail(RollbackDetail rollbackDetail) throws IOException {
    // Constructs the response object as GenericRecord
    Schema schema = Schemas.V1.PublishResponse.SCHEMA;
    GenericRecord record = new GenericData.Record(schema);
    record.put("transactionWritePointer", rollbackDetail.getTransactionWritePointer());

    GenericRecord rollbackRange = new GenericData.Record(schema.getField("rollbackRange").schema());
    rollbackRange.put("startTimestamp", rollbackDetail.getStartTimestamp());
    rollbackRange.put("startSequenceId", rollbackDetail.getStartSequenceId());
    rollbackRange.put("endTimestamp", rollbackDetail.getEndTimestamp());
    rollbackRange.put("endSequenceId", rollbackDetail.getEndSequenceId());

    record.put("rollbackRange", rollbackRange);

    ExposedByteArrayOutputStream os = new ExposedByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().directBinaryEncoder(os, null);

    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(Schemas.V1.PublishRequest.SCHEMA);
    datumWriter.write(record, encoder);

    return os.toByteBuffer();
  }

  /**
   * Verifies the content-type in the header matches with the required type.
   */
  private void verifyContentType(Map<String, ? extends Collection<String>> headers, String requiredContentType) {
    // Netty 4.1 has all headers set with lower case name
    // However, the cdap-http library doesn't handle it well, hence we iterate all headers instead of lookup in here
    boolean passed = false;
    for (Map.Entry<String, ? extends Collection<String>> entry : headers.entrySet()) {
      if (HttpHeaders.CONTENT_TYPE.equalsIgnoreCase(entry.getKey())) {
        passed = requiredContentType.equalsIgnoreCase(Iterables.getFirst(entry.getValue(), null));
        break;
      }
    }
    if (!passed) {
      throw new IllegalArgumentException("Only " + requiredContentType + " content type is support.");
    }
  }

  /**
   * A {@link ByteArrayOutputStream} that exposes the written raw buffer as ByteBuffer.
   */
  private static final class ExposedByteArrayOutputStream extends ByteArrayOutputStream {

    /**
     * Returns a {@link ByteBuffer} that represents the valid content in the buffer.
     */
    ByteBuffer toByteBuffer() {
      return ByteBuffer.wrap(buf, 0, count);
    }
  }

  /**
   * Client side implementation of {@link MessageFetcher}. It streams messages from the server with chunk encoding.
   */
  private final class ClientMessageFetcher extends MessageFetcher {

    private final TopicId topicId;
    private final DatumReader<GenericRecord> messageReader;
    private GenericRecord messageRecord;

    private ClientMessageFetcher(TopicId topicId) {
      this.topicId = topicId;

      // These are for reading individual message (response is an array of messages)
      this.messageRecord = new GenericData.Record(Schemas.V1.ConsumeResponse.SCHEMA.getElementType());
      this.messageReader = new GenericDatumReader<>(Schemas.V1.ConsumeResponse.SCHEMA.getElementType());
    }

    @Override
    public CloseableIterator<RawMessage> fetch() throws IOException, TopicNotFoundException {
      GenericRecord record = new GenericData.Record(Schemas.V1.ConsumeRequest.SCHEMA);

      if (getStartOffset() != null) {
        record.put("startFrom", ByteBuffer.wrap(getStartOffset()));
      }
      if (getStartTime() != null) {
        record.put("startFrom", getStartTime());
      }
      record.put("inclusive", isIncludeStart());
      record.put("limit", getLimit());

      if (getTransaction() != null) {
        record.put("transaction", ByteBuffer.wrap(TRANSACTION_CODEC.encode(getTransaction())));
      }

      // The cask common http library doesn't support read streaming, and we don't want to buffer all messages
      // in memory, hence we use the HttpURLConnection directly instead.
      HttpURLConnection urlConn = remoteClient.openConnection(HttpMethod.POST, createTopicPath(topicId) + "/poll");
      urlConn.setRequestProperty(HttpHeaders.CONTENT_TYPE, "avro/binary");
      if (compressPayload) {
        urlConn.setRequestProperty(HttpHeaders.ACCEPT_ENCODING, "gzip, deflate");
      }

      // Send the request
      Encoder encoder = EncoderFactory.get().directBinaryEncoder(urlConn.getOutputStream(), null);
      DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(Schemas.V1.ConsumeRequest.SCHEMA);
      datumWriter.write(record, encoder);

      int responseCode = urlConn.getResponseCode();
      if (responseCode == HttpURLConnection.HTTP_NOT_FOUND) {
        throw new TopicNotFoundException(topicId.getNamespace(), topicId.getTopic());
      }

      handleError(responseCode, () -> {
        // If there is any error, read the response body from the error stream
        try (InputStream errorStream = decompressIfNeeded(urlConn, urlConn.getErrorStream())) {
          return errorStream == null
            ? ""
            : urlConn.getResponseMessage() + new String(ByteStreams.toByteArray(errorStream),
                                                        StandardCharsets.UTF_8);
        } catch (IOException e) {
          return "";
        } finally {
          urlConn.disconnect();
        }
      }, "Failed to update topic " + topicId);
      verifyContentType(urlConn.getHeaderFields(), "avro/binary");

      // Decode the avro array manually instead of using DatumReader in order to support streaming decode.
      final InputStream inputStream = decompressIfNeeded(urlConn, urlConn.getInputStream());
      final Decoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
      final long initialItemCount = decoder.readArrayStart();
      return new AbstractCloseableIterator<RawMessage>() {

        private long itemCount = initialItemCount;

        @Override
        protected RawMessage computeNext() {
          if (initialItemCount == 0) {
            return endOfData();
          }

          try {
            if (itemCount == 0) {
              itemCount = decoder.arrayNext();
              if (itemCount == 0) {
                // The zero item count signals the end of the array
                return endOfData();
              }
            }

            itemCount--;

            // Use DatumReader to decode individual message
            // This provides greater flexibility on schema evolution.
            // The response will likely always be an array, but the element schema can evolve.
            messageRecord = messageReader.read(messageRecord, decoder);

            return new RawMessage(Bytes.toBytes((ByteBuffer) messageRecord.get("id")),
                                  Bytes.toBytes((ByteBuffer) messageRecord.get("payload")));
          } catch (IOException e) {
            throw Throwables.propagate(e);
          }
        }

        @Override
        public void close() {
          Closeables.closeQuietly(inputStream);
          urlConn.disconnect();
        }
      };
    }

    /**
     * Based on the given {@link HttpURLConnection} content encoding,
     * optionally wrap the given {@link InputStream} with either gzip or deflate decompression.
     */
    private InputStream decompressIfNeeded(HttpURLConnection urlConn, InputStream is) throws IOException {
      String contentEncoding = urlConn.getHeaderField(HttpHeaderNames.CONTENT_ENCODING.toString());
      if (contentEncoding == null) {
        return is;
      }

      if ("gzip".equalsIgnoreCase(contentEncoding)) {
        return new GZIPInputStream(is);
      }
      if ("deflate".equalsIgnoreCase(contentEncoding)) {
        return new DeflaterInputStream(is);
      }

      throw new IllegalArgumentException("Unsupported content encoding " + contentEncoding);
    }
  }
}
