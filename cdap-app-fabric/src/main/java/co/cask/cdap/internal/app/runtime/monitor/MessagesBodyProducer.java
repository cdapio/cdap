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

import co.cask.cdap.api.messaging.Message;
import co.cask.cdap.api.messaging.MessagingContext;
import co.cask.cdap.api.messaging.TopicNotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.LogSamplers;
import co.cask.cdap.common.logging.Loggers;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.http.BodyProducer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link BodyProducer} to encode and send back messages.
 * Instead of using GenericDatumWriter, we perform the map and array encoding manually so that we don't have to buffer
 * all messages in memory before sending out.
 */
final class MessagesBodyProducer extends BodyProducer {

  private static final Logger LOG = LoggerFactory.getLogger(MessagesBodyProducer.class);
  // For outage, only log once per 60 seconds per message.
  private static final Logger OUTAGE_LOG =  Loggers.sampling(LOG, LogSamplers.perMessage(
    () -> LogSamplers.limitRate(60000)));

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
  private Iterator<Message> iterator;
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
    if (iterator != null && iterator.hasNext()) {
      sendMessages();
    }

    return chunk.copy();
  }

  @Override
  public void finished() {
    chunk.release();
  }

  @Override
  public void handleError(@Nullable Throwable cause) {
    closeIterator(iterator);
    OUTAGE_LOG.error("Error occurred while sending chunks from Runtime Handler", cause);
  }

  /**
   * Returns the next iterator
   */
  private Iterator<Message> getIterator() throws IOException {
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


  private Iterator<Message> fetchMessages(Map.Entry<String, GenericRecord> requestEntry) throws IOException {
    String topic = getTopic(requestEntry.getKey());
    if (topic == null) {
      OUTAGE_LOG.warn("Ignoring topic config '{}' in the request that does not map to any local topic",
                      requestEntry.getKey());
      return Collections.emptyIterator();
    }

    int limit = (int) requestEntry.getValue().get("limit");
    String fromMessage = requestEntry.getValue().get("messageId") == null ? null :
      requestEntry.getValue().get("messageId").toString();

    try {
      return messagingContext.getMessageFetcher().fetch(NamespaceId.SYSTEM.getNamespace(), topic, limit, fromMessage);
    } catch (TopicNotFoundException e) {
      OUTAGE_LOG.warn("Ignoring topic '{}:{}' that does not exists locally", e.getNamespace(), e.getTopic());
      return Collections.emptyIterator();
    }
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
    closeIterator(iterator);
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

  @Nullable
  private String getTopic(String topicConfig) {
    int idx = topicConfig.lastIndexOf(':');
    if (idx < 0) {
      return cConf.get(topicConfig);
    }

    String prefix = cConf.get(topicConfig.substring(0, idx));
    return prefix == null ? null : prefix + topicConfig.substring(idx + 1);
  }

  private void closeIterator(Iterator<?> iterator) {
    if (iterator instanceof AutoCloseable) {
      try {
        ((AutoCloseable) iterator).close();
      } catch (Exception e) {
        LOG.warn("Exception raised when closing iterator", e);
      }
    }
  }
}
