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

package io.cdap.cdap.messaging.server;

import com.google.inject.Inject;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.SpillableBodyConsumer;
import io.cdap.cdap.common.io.ByteBuffers;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.RollbackDetail;
import io.cdap.cdap.messaging.Schemas;
import io.cdap.cdap.messaging.StoreRequest;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.BodyConsumer;
import io.cdap.http.HttpResponder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.List;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * A netty http handler for handling message storage REST API for the messaging system.
 */
@Path("/v1/namespaces/{namespace}/topics/{topic}")
public final class StoreHandler extends AbstractHttpHandler {

  private final java.nio.file.Path tempDir;
  private final int bufferSize;
  private final MessagingService messagingService;

  @Inject
  StoreHandler(CConfiguration cConf, MessagingService messagingService) {
    this.tempDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR), cConf.get(Constants.AppFabric.TEMP_DIR)).toPath();
    this.bufferSize = cConf.getInt(Constants.MessagingSystem.HTTP_SERVER_MAX_REQUEST_SIZE_MB) * 1024 * 1024;
    this.messagingService = messagingService;

    DirUtils.mkdirs(tempDir.toFile());
  }

  @POST
  @Path("/publish")
  public BodyConsumer publish(HttpRequest request, HttpResponder responder,
                              @PathParam("namespace") String namespace,
                              @PathParam("topic") String topic) throws Exception {

    TopicId topicId = new NamespaceId(namespace).topic(topic);
    return new SpillableBodyConsumer(Files.createTempFile(tempDir, "tms.publish", ".tmp"), bufferSize) {
      @Override
      protected void processInput(InputStream inputStream, HttpResponder responder) throws Exception {
        StoreRequest storeRequest = createStoreRequest(topicId, request, inputStream);
        // Empty payload is only allowed for transactional publish
        if (!storeRequest.isTransactional() && !storeRequest.hasPayload()) {
          throw new BadRequestException("Empty payload is only allowed for publishing transactional message. Topic: "
                                          + topicId);
        }
        // Publish the message and response with the rollback information
        RollbackDetail rollbackInfo = messagingService.publish(storeRequest);
        if (rollbackInfo == null) {
          // Non-tx publish doesn't have rollback info.
          responder.sendStatus(HttpResponseStatus.OK);
        } else {
          ByteBuf response = encodeRollbackDetail(rollbackInfo);
          responder.sendContent(HttpResponseStatus.OK, response,
                                new DefaultHttpHeaders().set(HttpHeaderNames.CONTENT_TYPE, "avro/binary"));
        }
      }
    };
  }

  @POST
  @Path("/store")
  public BodyConsumer store(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace") String namespace,
                            @PathParam("topic") String topic) throws Exception {

    TopicId topicId = new NamespaceId(namespace).topic(topic);
    return new SpillableBodyConsumer(Files.createTempFile(tempDir, "tms.store", ".tmp"), bufferSize) {
      @Override
      protected void processInput(InputStream inputStream, HttpResponder responder) throws Exception {
        StoreRequest storeRequest = createStoreRequest(topicId, request, inputStream);

        // It must be transactional with payload for store request
        if (!storeRequest.isTransactional() || !storeRequest.hasPayload()) {
          throw new BadRequestException("Store request must be transactional with payload. Topic: " + topicId);
        }

        messagingService.storePayload(storeRequest);
        responder.sendStatus(HttpResponseStatus.OK);
      }
    };
  }

  @POST
  @Path("/rollback")
  public void rollback(FullHttpRequest request, HttpResponder responder,
                       @PathParam("namespace") String namespace,
                       @PathParam("topic") String topic) throws Exception {
    TopicId topicId = new NamespaceId(namespace).topic(topic);

    Decoder decoder = DecoderFactory.get().directBinaryDecoder(new ByteBufInputStream(request.content()), null);
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(Schemas.V1.PublishResponse.SCHEMA);
    messagingService.rollback(topicId, new GenericRecordRollbackDetail(datumReader.read(null, decoder)));
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Creates a {@link StoreRequest} instance based on the given {@link HttpRequest}.
   */
  private StoreRequest createStoreRequest(TopicId topicId,
                                          HttpRequest request, InputStream is) throws BadRequestException, IOException {
    // Currently only support avro
    if (!"avro/binary".equals(request.headers().get(HttpHeaderNames.CONTENT_TYPE))) {
      throw new BadRequestException("Only avro/binary content type is supported.");
    }

    Decoder decoder = DecoderFactory.get().directBinaryDecoder(is, null);
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(Schemas.V1.PublishRequest.SCHEMA);
    return new GenericRecordStoreRequest(topicId, datumReader.read(null, decoder));
  }

  /**
   * Encodes the {@link RollbackDetail} object as avro record based on the {@link Schemas.V1.PublishResponse#SCHEMA}.
   */
  private ByteBuf encodeRollbackDetail(RollbackDetail rollbackDetail) throws IOException {
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
    ByteBuf buffer = Unpooled.buffer(38);
    Encoder encoder = EncoderFactory.get().directBinaryEncoder(new ByteBufOutputStream(buffer), null);
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
    datumWriter.write(response, encoder);
    return buffer;
  }

  /**
   * A {@link StoreRequest} that gets the request information from {@link GenericRecord}.
   */
  private static final class GenericRecordStoreRequest extends StoreRequest {

    private final List<ByteBuffer> payloads;

    @SuppressWarnings("unchecked")
    GenericRecordStoreRequest(TopicId topicId, GenericRecord record) {
      super(topicId,
            record.get("transactionWritePointer") != null,
            record.get("transactionWritePointer") == null
              ? -1L
              : Long.parseLong(record.get("transactionWritePointer").toString()));

      this.payloads = ((List<ByteBuffer>) record.get("messages"));
    }

    @Override
    public boolean hasPayload() {
      return !payloads.isEmpty();
    }

    @Override
    public Iterator<byte[]> iterator() {
      return payloads.stream().map(ByteBuffers::getByteArray).iterator();
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
