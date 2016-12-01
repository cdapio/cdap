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

import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.io.ByteBuffers;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.RollbackDetail;
import co.cask.cdap.messaging.Schemas;
import co.cask.cdap.messaging.StoreRequest;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.TopicId;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
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
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * A netty http handler for handling message storage REST API for the messaging system.
 */
@Path("/v1/namespaces/{namespace}/topics/{topic}")
public final class StoreHandler extends AbstractHttpHandler {

  private final MessagingService messagingService;

  @Inject
  StoreHandler(MessagingService messagingService) {
    this.messagingService = messagingService;
  }

  @POST
  @Path("/publish")
  public void publish(HttpRequest request, HttpResponder responder,
                      @PathParam("namespace") String namespace,
                      @PathParam("topic") String topic) throws Exception {

    TopicId topicId = new NamespaceId(namespace).topic(topic);
    StoreRequest storeRequest = createStoreRequest(topicId, request);

    // Empty payload is only allowed for transactional publish
    if (!storeRequest.isTransactional() && !storeRequest.hasNext()) {
      throw new BadRequestException("Empty payload is only allowed for publishing transactional message. Topic: "
                                      + topicId);
    }

    // Publish the message and response with the rollback information
    RollbackDetail rollbackInfo = messagingService.publish(storeRequest);
    if (rollbackInfo == null) {
      // Non-tx publish doesn't have rollback info.
      responder.sendStatus(HttpResponseStatus.OK);
      return;
    }
    ChannelBuffer response = encodeRollbackDetail(rollbackInfo);
    responder.sendContent(HttpResponseStatus.OK, response, "avro/binary", null);
  }

  @POST
  @Path("/store")
  public void store(HttpRequest request, HttpResponder responder,
                    @PathParam("namespace") String namespace,
                    @PathParam("topic") String topic) throws Exception {

    TopicId topicId = new NamespaceId(namespace).topic(topic);
    StoreRequest storeRequest = createStoreRequest(topicId, request);

    // It must be transactional with payload for store request
    if (!storeRequest.isTransactional() || !storeRequest.hasNext()) {
      throw new BadRequestException("Store request must be transactional with payload. Topic: " + topicId);
    }

    messagingService.storePayload(storeRequest);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/rollback")
  public void rollback(HttpRequest request, HttpResponder responder,
                       @PathParam("namespace") String namespace,
                       @PathParam("topic") String topic) throws Exception {
    TopicId topicId = new NamespaceId(namespace).topic(topic);

    Decoder decoder = DecoderFactory.get().directBinaryDecoder(new ChannelBufferInputStream(request.getContent()),
                                                               null);
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(Schemas.V1.PublishResponse.SCHEMA);
    messagingService.rollback(topicId, new GenericRecordRollbackDetail(datumReader.read(null, decoder)));
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Creates a {@link StoreRequest} instance based on the given {@link HttpRequest}.
   */
  private StoreRequest createStoreRequest(TopicId topicId, HttpRequest request) throws Exception {
    // Currently only support avro
    if (!"avro/binary".equals(request.getHeader(HttpHeaders.Names.CONTENT_TYPE))) {
      throw new BadRequestException("Only avro/binary content type is supported.");
    }

    Decoder decoder = DecoderFactory.get().directBinaryDecoder(new ChannelBufferInputStream(request.getContent()),
                                                               null);
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(Schemas.V1.PublishRequest.SCHEMA);
    return new GenericRecordStoreRequest(topicId, datumReader.read(null, decoder));
  }

  /**
   * Encodes the {@link RollbackDetail} object as avro record based on the {@link Schemas.V1.PublishResponse#SCHEMA}.
   */
  private ChannelBuffer encodeRollbackDetail(RollbackDetail rollbackDetail) throws IOException {
    Schema schema = Schemas.V1.PublishResponse.SCHEMA;

    // Constructs the response object as GenericRecord
    GenericRecord response = new GenericData.Record(schema);
    response.put("transactionWritePointer", rollbackDetail.getTransactionWritePointer());

    GenericRecord rollbackRange = new GenericData.Record(schema.getField("rollbackRange").schema());
    rollbackRange.put("startTimestamp", rollbackDetail.getStartTimestamp());
    rollbackRange.put("startSequenceId", rollbackDetail.getStartSequenceId());
    rollbackRange.put("endTimestamp", rollbackDetail.getEndTimestamp());
    rollbackRange.put("endSequenceId", rollbackDetail.getEndSequenceId());

    response.put("rollbackRange", rollbackRange);

    // For V1 PublishResponse, it contains an union(long, null) and then 2 longs and 2 integers,
    // hence the max size is 38
    // (union use 1 byte, long max size is 9 bytes, integer max size is 5 bytes in avro binary encoding)
    ChannelBuffer buffer = ChannelBuffers.dynamicBuffer(38);
    Encoder encoder = EncoderFactory.get().directBinaryEncoder(new ChannelBufferOutputStream(buffer), null);
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
    datumWriter.write(response, encoder);
    return buffer;
  }

  /**
   * A {@link StoreRequest} that gets the request information from {@link GenericRecord}.
   */
  private static final class GenericRecordStoreRequest extends StoreRequest {

    private final Iterator<ByteBuffer> payloadIterator;

    @SuppressWarnings("unchecked")
    GenericRecordStoreRequest(TopicId topicId, GenericRecord genericRecord) {
      super(topicId, genericRecord.get("transactionWritePointer") != null,
            genericRecord.get("transactionWritePointer") == null
              ? -1L
              : Long.parseLong(genericRecord.get("transactionWritePointer").toString()));

      this.payloadIterator = ((List<ByteBuffer>) genericRecord.get("messages")).iterator();
    }

    @Nullable
    @Override
    protected byte[] doComputeNext() {
      if (!payloadIterator.hasNext()) {
        return null;
      }
      return ByteBuffers.getByteArray(payloadIterator.next());
    }
  }

  /**
   * A {@link RollbackDetail} implementation that is backed by a {@link GenericRecord} with the
   * {@link Schemas.V1.PublishResponse#SCHEMA}.
   */
  private static final class GenericRecordRollbackDetail implements RollbackDetail {

    private final GenericRecord record;

    private GenericRecordRollbackDetail(GenericRecord record) {
      this.record = record;
    }

    @Override
    public long getTransactionWritePointer() {
      return (Long) record.get("transactionWritePointer");
    }

    @Override
    public long getStartTimestamp() {
      return (Long) ((GenericRecord) record.get("rollbackRange")).get("startTimestamp");
    }

    @Override
    public int getStartSequenceId() {
      return (Integer) ((GenericRecord) record.get("rollbackRange")).get("startSequenceId");
    }

    @Override
    public long getEndTimestamp() {
      return (Long) ((GenericRecord) record.get("rollbackRange")).get("endTimestamp");
    }

    @Override
    public int getEndSequenceId() {
      return (Integer) ((GenericRecord) record.get("rollbackRange")).get("endSequenceId");
    }
  }
}
