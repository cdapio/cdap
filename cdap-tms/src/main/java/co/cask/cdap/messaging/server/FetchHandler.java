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

package co.cask.cdap.messaging.server;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.ByteBuffers;
import co.cask.cdap.messaging.MessageFetcher;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.Schemas;
import co.cask.cdap.messaging.TopicNotFoundException;
import co.cask.cdap.messaging.data.Message;
import co.cask.cdap.messaging.data.MessageId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.TopicId;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.BodyProducer;
import co.cask.http.HttpResponder;
import com.google.common.collect.ImmutableMultimap;
import com.google.inject.Inject;
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
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * A netty http handler for handling message fetching REST API for the messaging system.
 */
@Path("/v1/namespaces/{namespace}/topics/{topic}")
public final class FetchHandler extends AbstractHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(FetchHandler.class);
  private static final TransactionCodec TRANSACTION_CODEC = new TransactionCodec();

  private final MessagingService messagingService;
  private int messageChunkSize;

  @Inject
  FetchHandler(CConfiguration cConf, MessagingService messagingService) {
    this.messagingService = messagingService;
    this.messageChunkSize = cConf.getInt(Constants.MessagingSystem.HTTP_SERVER_CONSUME_CHUNK_SIZE);
  }

  @POST
  @Path("poll")
  public void poll(HttpRequest request, HttpResponder responder,
                   @PathParam("namespace") String namespace,
                   @PathParam("topic") String topic) throws Exception {

    TopicId topicId = new NamespaceId(namespace).topic(topic);

    // Currently only support avro
    if (!"avro/binary".equals(request.getHeader(HttpHeaders.Names.CONTENT_TYPE))) {
      throw new BadRequestException("Only avro/binary content type is supported.");
    }

    // Decode the poll request
    Decoder decoder = DecoderFactory.get().directBinaryDecoder(new ChannelBufferInputStream(request.getContent()),
                                                               null);
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(Schemas.V1.ConsumeRequest.SCHEMA);

    // Fetch the messages
    CloseableIterator<Message> iterator = fetchMessages(datumReader.read(null, decoder), topicId);
    try {
      responder.sendContent(HttpResponseStatus.OK, new MessagesBodyProducer(iterator, messageChunkSize),
                            ImmutableMultimap.of(HttpHeaders.Names.CONTENT_TYPE, "avro/binary"));
    } catch (Throwable t) {
      iterator.close();
      throw t;
    }
  }

  /**
   * Creates a {@link CloseableIterator} of {@link Message} based on the given fetch request.
   */
  private CloseableIterator<Message> fetchMessages(GenericRecord fetchRequest,
                                                   TopicId topicId) throws IOException, TopicNotFoundException {
    MessageFetcher fetcher = messagingService.prepareFetch(topicId);

    Object startFrom = fetchRequest.get("startFrom");
    if (startFrom != null) {
      if (startFrom instanceof ByteBuffer) {
        // start message id is specified
        fetcher.setStartMessage(new MessageId(Bytes.toBytes((ByteBuffer) startFrom)),
                                (Boolean) fetchRequest.get("inclusive"));
      } else if (startFrom instanceof Long) {
        // start by timestamp is specified
        fetcher.setStartTime((Long) startFrom);
      } else {
        // This shouldn't happen as it's guaranteed by the schema
        LOG.warn("Ignore unrecognized type for startFrom. Type={}, Value={}", startFrom.getClass(), startFrom);
      }
    }

    Integer limit = (Integer) fetchRequest.get("limit");
    if (limit != null) {
      fetcher.setLimit(limit);
    }

    ByteBuffer encodedTx = (ByteBuffer) fetchRequest.get("transaction");
    if (encodedTx != null) {
      fetcher.setTransaction(TRANSACTION_CODEC.decode(ByteBuffers.getByteArray(encodedTx)));
    }

    return fetcher.fetch();
  }

  /**
   * A {@link BodyProducer} to encode and send back messages.
   * Instead of using GenericDatumWriter, we perform the array encoding manually so that we don't have to buffer
   * all messages in memory before sending out.
   */
  private static class MessagesBodyProducer extends BodyProducer {

    private final CloseableIterator<Message> iterator;
    private final List<Message> messages;
    private final int messageChunkSize;
    private final ChannelBuffer chunk;
    private final Encoder encoder;
    private final GenericRecord messageRecord;
    private final DatumWriter<GenericRecord> messageWriter;
    private boolean arrayStarted;
    private boolean arrayEnded;

    MessagesBodyProducer(CloseableIterator<Message> iterator, int messageChunkSize) {
      this.iterator = iterator;
      this.messages = new ArrayList<>();
      this.messageChunkSize = messageChunkSize;
      this.chunk = ChannelBuffers.dynamicBuffer(messageChunkSize);
      this.encoder = EncoderFactory.get().directBinaryEncoder(new ChannelBufferOutputStream(chunk), null);

      // These are for writing individual message (response is an array of messages)
      this.messageRecord = new GenericData.Record(Schemas.V1.ConsumeResponse.SCHEMA.getElementType());
      this.messageWriter = new GenericDatumWriter<GenericRecord>(Schemas.V1.ConsumeResponse.SCHEMA.getElementType()) {
        @Override
        protected void writeBytes(Object datum, Encoder out) throws IOException {
          if (datum instanceof byte[]) {
            out.writeBytes((byte[]) datum);
          } else {
            super.writeBytes(datum, out);
          }
        }
      };
    }

    @Override
    public ChannelBuffer nextChunk() throws Exception {
      // Already sent all messages, return empty to signal the end of response
      if (arrayEnded) {
        return ChannelBuffers.EMPTY_BUFFER;
      }

      chunk.clear();

      if (!arrayStarted) {
        arrayStarted = true;
        encoder.writeArrayStart();
      }

      // Try to buffer up to buffer size
      int size = 0;
      messages.clear();
      while (iterator.hasNext() && size < messageChunkSize) {
        Message message = iterator.next();
        messages.add(message);

        // Avro encodes bytes as (len + bytes), hence adding 8 to cater for the length of the id and payload
        // Straightly speaking it can be up to 9 bytes each (hence 18 bytes),
        // but we don't expect id and payload of such size
        size += message.getId().getRawId().length + message.getPayload().length + 8;
      }

      encoder.setItemCount(messages.size());
      for (Message message : messages) {
        encoder.startItem();

        // Write individual message (array element) with DatumWrite.
        // This provides greater flexibility on schema evolution.
        // The response will likely always be an array, but the element schema can evolve.
        messageRecord.put("id", message.getId().getRawId());
        messageRecord.put("payload", message.getPayload());
        messageWriter.write(messageRecord, encoder);
      }

      if (!iterator.hasNext()) {
        arrayEnded = true;
        encoder.writeArrayEnd();
      }

      return chunk;
    }

    @Override
    public void finished() throws Exception {
      iterator.close();
    }

    @Override
    public void handleError(@Nullable Throwable cause) {
      iterator.close();
      // Since response header is already sent, there is nothing we can send back to client. Simply log the failure
      if (cause instanceof SocketException) {
        // This can easily caused by client close connection prematurely. Don't want to flood the log.
        LOG.debug("Socket exception raised when sending messages back to client", cause);
      } else {
        LOG.warn("Exception raised when sending messages back to client", cause);
      }
    }
  }
}
