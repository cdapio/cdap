/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.monitor;

import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.messaging.Message;
import co.cask.cdap.api.messaging.MessagingContext;
import co.cask.cdap.api.messaging.TopicNotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.LogSamplers;
import co.cask.cdap.common.logging.Loggers;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.BodyProducer;
import co.cask.http.HttpResponder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import javax.annotation.Nullable;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * {@link co.cask.http.HttpHandler} for exposing metadata of a runtime.
 */
@Path("/v1/runtime")
public class RuntimeHandler extends AbstractHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(RuntimeHandler.class);
  // For outage, only log once per 60 seconds per message.
  private static final Logger OUTAGE_LOG = Loggers.sampling(LOG, LogSamplers.perMessage(
    () -> LogSamplers.limitRate(60000)));

  private final CConfiguration cConf;
  private final MessagingContext messagingContext;
  private final Runnable shutdownRunnable;

  public RuntimeHandler(CConfiguration cConf, MessagingContext messagingContext, Runnable shutdownRunnable) {
    this.cConf = cConf;
    this.messagingContext = messagingContext;
    this.shutdownRunnable = shutdownRunnable;
  }

  /**
   * Gets list of topics along with offsets and limit as request and returns list of messages
   */
  @POST
  @Path("/metadata")
  public void metadata(FullHttpRequest request, HttpResponder responder) throws Exception {
    Map<String, GenericRecord> consumeRequests = decodeConsumeRequest(request);
    MessagesBodyProducer messagesBodyProducer = new MessagesBodyProducer(cConf, consumeRequests, messagingContext);
    responder.sendContent(HttpResponseStatus.OK, messagesBodyProducer,
                          new DefaultHttpHeaders().set(HttpHeaderNames.CONTENT_TYPE, "avro/binary"));
  }

  /**
   * shuts down remote runtime
   */
  @POST
  @Path("/shutdown")
  public void shutdown(FullHttpRequest request, HttpResponder responder) throws Exception {
    responder.sendString(HttpResponseStatus.OK, "Triggering shutdown down Runtime Http Server.");
    shutdownRunnable.run();
  }

  /**
   * Decode consume request from avro binary format
   */
  private Map<String, GenericRecord> decodeConsumeRequest(FullHttpRequest request) throws IOException {
    Decoder decoder = DecoderFactory.get().directBinaryDecoder(new ByteBufInputStream(request.content()), null);
    DatumReader<Map<String, GenericRecord>> datumReader = new GenericDatumReader<>(
      MonitorSchemas.V1.MonitorConsumeRequest.SCHEMA);
    return datumReader.read(null, decoder);
  }


  /**
   * A {@link BodyProducer} to encode and send back messages.
   * Instead of using GenericDatumWriter, we perform the map and array encoding manually so that we don't have to buffer
   * all messages in memory before sending out.
   */
  private static class MessagesBodyProducer extends BodyProducer {
    private final Iterator<Map.Entry<String, GenericRecord>> requestsIterator;
    private final MessagingContext messagingContext;
    private final CConfiguration cConf;
    private final Schema elementSchema = MonitorSchemas.V1.MonitorResponse.SCHEMA.getValueType().getElementType();
    private final int numOfRequests;
    private final int messageChunkSize;
    private final Deque<GenericRecord> monitorMessages;
    private final ByteBuf chunk;
    private final Encoder encoder;
    private final DatumWriter<GenericRecord> messageWriter;
    private CloseableIterator<Message> iterator;
    private boolean mapStarted;
    private boolean mapEnded;

    MessagesBodyProducer(CConfiguration cConf, Map<String, GenericRecord> requests,
                         MessagingContext messagingContext) {
      this.requestsIterator = requests.entrySet().iterator();
      this.messagingContext = messagingContext;
      this.cConf = cConf;
      this.numOfRequests = requests.size();
      this.messageChunkSize = cConf.getInt(Constants.RuntimeMonitor.SERVER_CONSUME_CHUNK_SIZE);
      this.monitorMessages = new LinkedList<>();
      this.chunk = Unpooled.buffer(messageChunkSize);
      this.encoder = EncoderFactory.get().directBinaryEncoder(new ByteBufOutputStream(chunk), null);
      this.messageWriter = new GenericDatumWriter<GenericRecord>(elementSchema) {
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
    public ByteBuf nextChunk() throws Exception {
      // Already sent all messages, return empty to signal the end of response
      if (mapEnded) {
        return Unpooled.EMPTY_BUFFER;
      }
      chunk.clear();

      iterator = getIterator();
      if (iterator.hasNext()) {
        sendMessages();
      }

      return chunk.copy();
    }

    @Override
    public void finished() throws Exception {
      chunk.release();
    }

    @Override
    public void handleError(@Nullable Throwable cause) {
      if (iterator != null) {
        iterator.close();
      }
      OUTAGE_LOG.error("Error occurred while sending chunks from Runtime Handler", cause);
    }

    /**
     * Returns the next iterator
     */
    private CloseableIterator<Message> getIterator() throws IOException, TopicNotFoundException {
      if (!mapStarted) {
        startMap();
      }

      if (iterator == null || !iterator.hasNext()) {
        if (iterator != null) {
          endArray();
        }

        if (requestsIterator.hasNext()) {
          Map.Entry<String, GenericRecord> requestEntry = requestsIterator.next();
          startArray(requestEntry.getKey());
          iterator = fetchMessages(requestEntry);
        } else {
          endMap();
        }
      }

      return iterator;
    }


    private CloseableIterator<Message> fetchMessages(Map.Entry<String, GenericRecord> requestEntry)
      throws TopicNotFoundException, IOException {
      String topic = getTopic(requestEntry.getKey());
      int limit = (int) requestEntry.getValue().get("limit");
      String fromMessage = requestEntry.getValue().get("messageId") == null ? null :
        requestEntry.getValue().get("messageId").toString();
      return messagingContext.getMessageFetcher().fetch(NamespaceId.SYSTEM.getNamespace(), topic, limit, fromMessage);
    }

    /**
     * Buffers and counts number of messages to be sent in this chunk
     */
    private void bufferMessagesToSend() {
      int size = 0;
      while (iterator.hasNext() && size < messageChunkSize) {
        Message rawMessage = iterator.next();
        // Avro requires number of objects to be written first so we will have to buffer messages
        GenericRecord record = createGenericRecord(rawMessage);
        monitorMessages.addLast(record);
        // Avro prefixes string and byte array with its length which is a 32 bit int. So add 8 bytes to size for
        // correct calculation of number bytes on the buffer.
        size += rawMessage.getId().length() + rawMessage.getPayload().length + 8;
      }
    }

    private void startMap() throws IOException {
      encoder.writeMapStart();
      encoder.setItemCount(numOfRequests);
      mapStarted = true;
    }

    private void startArray(String key) throws IOException {
      encoder.startItem();
      encoder.writeString(key);
      encoder.writeArrayStart();
    }

    private void endArray() throws IOException {
      // close this iterator before moving to next one.
      iterator.close();
      encoder.writeArrayEnd();
    }

    private void endMap() throws IOException {
      // end the map when all the request entries have been processed
      encoder.writeMapEnd();
      mapEnded = true;
    }

    /**
     * Sends messages using message writer
     */
    private void sendMessages() throws IOException {
      bufferMessagesToSend();
      encoder.setItemCount(monitorMessages.size());
      for (GenericRecord monitorMessage : monitorMessages) {
        encoder.startItem();
        messageWriter.write(monitorMessage, encoder);
      }
      monitorMessages.clear();
    }

    private GenericRecord createGenericRecord(Message rawMessage) {
      GenericRecord record = new GenericData.Record(elementSchema);
      record.put("messageId", rawMessage.getId());
      record.put("message", rawMessage.getPayload());
      return record;
    }

    private String getTopic(String topicConfig) {
      int idx = topicConfig.lastIndexOf(':');
      return idx < 0 ? cConf.get(topicConfig) : cConf.get(topicConfig.substring(0, idx)) +
        topicConfig.substring(idx + 1);
    }
  }
}
