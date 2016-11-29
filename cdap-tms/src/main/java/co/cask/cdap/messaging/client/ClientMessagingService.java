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

package co.cask.cdap.messaging.client;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.AbstractCloseableIterator;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.common.ServiceUnavailableException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.EndpointStrategy;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.common.http.DefaultHttpRequestConfig;
import co.cask.cdap.messaging.MessageFetcher;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.RollbackDetail;
import co.cask.cdap.messaging.Schemas;
import co.cask.cdap.messaging.StoreRequest;
import co.cask.cdap.messaging.TopicAlreadyExistsException;
import co.cask.cdap.messaging.TopicMetadata;
import co.cask.cdap.messaging.TopicNotFoundException;
import co.cask.cdap.messaging.data.Message;
import co.cask.cdap.messaging.data.MessageId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.TopicId;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequestConfig;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.io.ByteStreams;
import com.google.common.net.HttpHeaders;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
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
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * The client implementation of {@link MessagingService}. This client is intended for internal
 * higher level API implementation only.
 *
 * NOTE: This class shouldn't expose to end user (e.g. cdap-client module).
 */
public final class ClientMessagingService implements MessagingService {

  // A arbitrary timeout for getting endpoints from discovery service.
  // It is mainly for the first discovery call as it takes time for ZK to mirror the changes.
  private static final long DISCOVERY_PICK_TIMEOUT_SECS = 5L;
  private static final HttpRequestConfig HTTP_REQUEST_CONFIG = new DefaultHttpRequestConfig();
  private static final TransactionCodec TRANSACTION_CODEC = new TransactionCodec();
  private static final Gson GSON = new Gson();
  // These types for only for Gson to use, hence using the gson TypeToken instead of guava one
  private static final Type TOPIC_PROPERTY_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Type TOPIC_LIST_TYPE = new TypeToken<List<String>>() { }.getType();

  private final EndpointStrategy endpointStrategy;

  @VisibleForTesting
  @Inject
  public ClientMessagingService(DiscoveryServiceClient discoveryServiceClient) {
    this.endpointStrategy = new RandomEndpointStrategy(
      discoveryServiceClient.discover(Constants.Service.MESSAGING_SERVICE));
  }

  @Override
  public void createTopic(TopicMetadata topicMetadata) throws TopicAlreadyExistsException, IOException {
    TopicId topicId = topicMetadata.getTopicId();

    HttpRequest request = HttpRequest
      .put(createURL(createTopicPath(topicId)))
      .withBody(GSON.toJson(topicMetadata.getProperties()))
      .build();
    HttpResponse response = HttpRequests.execute(request, HTTP_REQUEST_CONFIG);

    if (response.getResponseCode() == HttpURLConnection.HTTP_CONFLICT) {
      throw new TopicAlreadyExistsException(topicId);
    }
    handleError(response, "Failed to create topic " + topicId);
  }

  @Override
  public void updateTopic(TopicMetadata topicMetadata) throws TopicNotFoundException, IOException {
    TopicId topicId = topicMetadata.getTopicId();

    HttpRequest request = HttpRequest
      .put(createURL(createTopicPath(topicId) + "/properties"))
      .withBody(GSON.toJson(topicMetadata.getProperties()))
      .build();
    HttpResponse response = HttpRequests.execute(request, HTTP_REQUEST_CONFIG);

    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new TopicNotFoundException(topicId);
    }
    handleError(response, "Failed to update topic " + topicId);
  }

  @Override
  public void deleteTopic(TopicId topicId) throws TopicNotFoundException, IOException {
    HttpRequest request = HttpRequest
      .delete(createURL(createTopicPath(topicId)))
      .build();
    HttpResponse response = HttpRequests.execute(request, HTTP_REQUEST_CONFIG);

    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new TopicNotFoundException(topicId);
    }
    handleError(response, "Failed to update topic " + topicId);
  }

  @Override
  public TopicMetadata getTopic(TopicId topicId) throws TopicNotFoundException, IOException {
    HttpRequest request = HttpRequest
      .get(createURL(createTopicPath(topicId)))
      .build();
    HttpResponse response = HttpRequests.execute(request, HTTP_REQUEST_CONFIG);

    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new TopicNotFoundException(topicId);
    }
    handleError(response, "Failed to update topic " + topicId);

    Map<String, String> properties = GSON.fromJson(response.getResponseBodyAsString(), TOPIC_PROPERTY_TYPE);
    return new TopicMetadata(topicId, properties);
  }

  @Override
  public List<TopicId> listTopics(NamespaceId namespaceId) throws IOException {
    HttpRequest request = HttpRequest
      .get(createURL(namespaceId.getNamespace() + "/topics"))
      .build();
    HttpResponse response = HttpRequests.execute(request, HTTP_REQUEST_CONFIG);

    handleError(response, "Failed to list topics in namespace " + namespaceId);
    List<String> topics = GSON.fromJson(response.getResponseBodyAsString(), TOPIC_LIST_TYPE);
    List<TopicId> result = new ArrayList<>(topics.size());
    for (String topic : topics) {
      result.add(namespaceId.topic(topic));
    }

    return Collections.unmodifiableList(result);
  }

  @Override
  public MessageFetcher prepareFetch(TopicId topicId) throws TopicNotFoundException, IOException {
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

    HttpRequest httpRequest = HttpRequest
      .post(createURL(createTopicPath(topicId) + "/rollback"))
      .addHeader(HttpHeaders.CONTENT_TYPE, "avro/binary")
      .withBody(requestBody)
      .build();

    HttpResponse response = HttpRequests.execute(httpRequest, HTTP_REQUEST_CONFIG);

    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new TopicNotFoundException(topicId);
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
    Encoder encoder = EncoderFactory.get().directBinaryEncoder(os, null);

    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(Schemas.V1.PublishRequest.SCHEMA);
    datumWriter.write(record, encoder);

    // Make the publish request
    String writeType = publish ? "publish" : "store";
    TopicId topicId = request.getTopicId();
    HttpRequest httpRequest = HttpRequest
      .post(createURL(createTopicPath(topicId) + "/" + writeType))
      .addHeader(HttpHeaders.CONTENT_TYPE, "avro/binary")
      .withBody(os.toByteBuffer())
      .build();

    HttpResponse response = HttpRequests.execute(httpRequest, HTTP_REQUEST_CONFIG);

    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new TopicNotFoundException(topicId);
    }
    handleError(response, "Failed to " + writeType + " message to topic " + topicId);
    return response;
  }

  /**
   * Creates the URL path for making HTTP requests for the given topic.
   */
  private String createTopicPath(TopicId topicId) {
    return topicId.getNamespace() + "/topics/" + topicId.getTopic();
  }

  /**
   * Creates a URL for making HTTP requests to the messaging system.
   */
  private URL createURL(String path) throws MalformedURLException {
    Discoverable discoverable = endpointStrategy.pick(DISCOVERY_PICK_TIMEOUT_SECS, TimeUnit.SECONDS);
    if (discoverable == null) {
      throw new ServiceUnavailableException("No endpoint available for messaging service");
    }

    InetSocketAddress address = discoverable.getSocketAddress();

    URI baseURI = URI.create(String.format("http://%s:%d/v1/namespaces/", address.getHostName(), address.getPort()));
    return baseURI.resolve(path).toURL();
  }

  /**
   * Handles error response from the given {@link HttpResponse}.
   */
  private void handleError(final HttpResponse response, String errorPrefix) throws IOException {
    handleError(response.getResponseCode(), new Supplier<String>() {
      @Override
      public String get() {
        return response.getResponseBodyAsString();
      }
    }, errorPrefix);
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
    List<ByteBuffer> payloads = new ArrayList<>();
    while (request.hasNext()) {
      payloads.add(ByteBuffer.wrap(request.next()));
    }
    return payloads;
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
    Collection<String> values = headers.get(HttpHeaders.CONTENT_TYPE);
    if (values == null || !requiredContentType.equals(Iterables.getFirst(values, null))) {
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
    public CloseableIterator<Message> fetch() throws IOException, TopicNotFoundException {
      GenericRecord record = new GenericData.Record(Schemas.V1.ConsumeRequest.SCHEMA);

      if (getStartOffset() != null) {
        record.put("startFrom", ByteBuffer.wrap(getStartOffset().getRawId()));
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
      URL url = createURL(createTopicPath(topicId) + "/poll");
      final HttpURLConnection urlConn = (HttpURLConnection)  url.openConnection();
      urlConn.setConnectTimeout(HTTP_REQUEST_CONFIG.getConnectTimeout());
      urlConn.setReadTimeout(HTTP_REQUEST_CONFIG.getReadTimeout());
      urlConn.setRequestMethod("POST");
      urlConn.setRequestProperty(HttpHeaders.CONTENT_TYPE, "avro/binary");
      urlConn.setDoInput(true);
      urlConn.setDoOutput(true);

      // Send the request
      Encoder encoder = EncoderFactory.get().directBinaryEncoder(urlConn.getOutputStream(), null);
      DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(Schemas.V1.ConsumeRequest.SCHEMA);
      datumWriter.write(record, encoder);

      int responseCode = urlConn.getResponseCode();
      if (responseCode == HttpURLConnection.HTTP_NOT_FOUND) {
        throw new TopicNotFoundException(topicId);
      }

      handleError(responseCode, new Supplier<String>() {
        @Override
        public String get() {
          // If there is any error, read the response body from the error stream
          InputStream errorStream = urlConn.getErrorStream();
          try {
            return errorStream == null ? "" : new String(ByteStreams.toByteArray(errorStream),
                                                         StandardCharsets.UTF_8);
          } catch (IOException e) {
            return "";
          }
        }
      }, "Failed to update topic " + topicId);
      verifyContentType(urlConn.getHeaderFields(), "avro/binary");

      // Decode the avro array manually instead of using DatumReader in order to support streaming decode.
      final Decoder decoder = DecoderFactory.get().binaryDecoder(urlConn.getInputStream(), null);
      final long initialItemCount = decoder.readArrayStart();
      return new AbstractCloseableIterator<Message>() {

        private long itemCount = initialItemCount;

        @Override
        protected Message computeNext() {
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

            return new Message(new MessageId(Bytes.toBytes((ByteBuffer) messageRecord.get("id"))),
                               Bytes.toBytes((ByteBuffer) messageRecord.get("payload")));
          } catch (IOException e) {
            throw Throwables.propagate(e);
          }
        }

        @Override
        public void close() {
          urlConn.disconnect();
        }
      };
    }
  }
}
