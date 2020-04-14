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

import com.google.common.io.Closeables;
import com.google.inject.Inject;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.messaging.MessagingContext;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.BodyConsumer;
import io.cdap.http.HandlerContext;
import io.cdap.http.HttpResponder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * The http handler for handling runtime requests from the program runtime.
 */
@Path(Constants.Gateway.INTERNAL_API_VERSION_3 +
  "/runtime/namespaces/{namespace}/apps/{app}/versions/{version}/{program-type}/{program}/runs/{run}")
public class RuntimeHandler extends AbstractHttpHandler {

  private final MessagingContext messagingContext;
  private final RuntimeRequestValidator requestValidator;
  private final RemoteExecutionLogProcessor logProcessor;
  private final String logsTopicPrefix;

  @Inject
  RuntimeHandler(CConfiguration cConf, MessagingService messagingService,
                 RemoteExecutionLogProcessor logProcessor, RuntimeRequestValidator requestValidator) {
    this.requestValidator = requestValidator;
    this.logProcessor = logProcessor;
    this.messagingContext = new MultiThreadMessagingContext(messagingService);
    this.logsTopicPrefix = cConf.get(Constants.Logging.TMS_TOPIC_PREFIX);
  }

  @Override
  public void init(HandlerContext context) {
    super.init(context);

    // Make sure the schema is what we expected
    Schema schema = MonitorSchemas.V2.MonitorRequest.SCHEMA;
    if (schema == null) {
      throw new IllegalStateException("Missing MonitorRequest schema");
    }
    if (schema.getType() != Schema.Type.ARRAY || schema.getElementType().getType() != Schema.Type.BYTES) {
      throw new IllegalStateException("MonitorRequest schema should be an array of bytes");
    }
  }

  /**
   * Handles call for writing to TMS from the program runtime for a given program run. The POST body is an
   * avro array of bytes.
   */
  @Path("/topics/{topic}")
  @POST
  public BodyConsumer writeMessages(HttpRequest request, HttpResponder responder,
                                    @PathParam("namespace") String namespace,
                                    @PathParam("app") String app,
                                    @PathParam("version") String version,
                                    @PathParam("program-type") String programType,
                                    @PathParam("program") String program,
                                    @PathParam("run") String run,
                                    @PathParam("topic") String topic) throws Exception {

    if (!"avro/binary".equals(request.headers().get(HttpHeaderNames.CONTENT_TYPE))) {
      throw new BadRequestException("Only avro/binary content type is supported.");
    }

    ApplicationId appId = new NamespaceId(namespace).app(app, version);
    ProgramRunId programRunId = new ProgramRunId(appId,
                                                 ProgramType.valueOfCategoryName(programType, BadRequestException::new),
                                                 program, run);
    requestValidator.validate(programRunId, request);

    TopicId topicId = NamespaceId.SYSTEM.topic(topic);
    if (topic.startsWith(logsTopicPrefix)) {
      return new MessageBodyConsumer(topicId, logProcessor::process);
    }

    return new MessageBodyConsumer(topicId, payloads -> {
      try {
        messagingContext.getDirectMessagePublisher().publish(topicId.getNamespace(),
                                                             topicId.getTopic(), payloads);
      } catch (TopicNotFoundException e) {
        throw new BadRequestException(e);
      }
    });
  }

  /**
   * A {@link BodyConsumer} to consume request from program runtime for writing messages to TMS.
   * It decodes and write messages to TMS in a streaming micro-batching fashion.
   */
  private static final class MessageBodyConsumer extends BodyConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(MessageBodyConsumer.class);

    private final TopicId topicId;
    private final PayloadProcessor payloadProcessor;
    private final CompositeByteBuf buffer;
    private final DelegatingInputStream inputStream;
    private final Decoder decoder;
    private final List<byte[]> payloads;
    private ByteBuffer payload;
    private long items;

    MessageBodyConsumer(TopicId topicId, PayloadProcessor payloadProcessor) {
      this.topicId = topicId;
      this.payloadProcessor = payloadProcessor;
      this.buffer = Unpooled.compositeBuffer();
      this.inputStream = new DelegatingInputStream(new ByteBufInputStream(buffer));
      this.decoder = DecoderFactory.get().directBinaryDecoder(inputStream, null);
      this.payloads = new LinkedList<>();
      this.items = -1L;
    }

    @Override
    public void chunk(ByteBuf request, HttpResponder responder) {
      buffer.discardReadComponents();
      buffer.addComponent(true, request.retain());
      inputStream.setDelegate(new ByteBufInputStream(buffer));
      try {
        try {
          if (items < 0) {
            // Read the initial array block
            inputStream.mark(buffer.readableBytes());
            items = decoder.readArrayStart();
          }

          // Decode element in the current array block
          while (items > 0) {
            inputStream.mark(buffer.readableBytes());
            payload = decoder.readBytes(payload);
            payloads.add(Bytes.toBytes(payload));
            items--;
          }

          if (!payloads.isEmpty()) {
            try {
              payloadProcessor.process(payloads.iterator());
              payloads.clear();
            } catch (IOException e) {
              // If we cannot process, just continue to keep buffering messages and retry at the next/finished called.
              LOG.debug("Failed to process payload for topic {}. Will be retried", topicId, e);
            }
          }

          // Read the next array block
          inputStream.mark(buffer.readableBytes());
          items = decoder.arrayNext();
        } catch (EOFException e) {
          inputStream.reset();
        }
      } catch (IOException | BadRequestException e) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST,
                             "Failed to process request due to exception " + e.getMessage());
        throw new RuntimeException(e);
      }
    }

    @Override
    public void finished(HttpResponder responder) {
      try {
        if (payloads.isEmpty()) {
          responder.sendStatus(HttpResponseStatus.OK);
          return;
        }
        try {
          payloadProcessor.process(payloads.iterator());
          responder.sendStatus(HttpResponseStatus.OK);
        } catch (BadRequestException e) {
          responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
        } catch (IOException e) {
          responder.sendString(HttpResponseStatus.SERVICE_UNAVAILABLE,
                               "Failed to process all messages due to " + e.getMessage());
        }
      } finally {
        Closeables.closeQuietly(inputStream);
        buffer.release();
      }
    }

    @Override
    public void handleError(Throwable cause) {
      LOG.error("Exception raised when processing message body for publishing to topic {}", topicId, cause);
    }
  }

  /**
   * An {@link InputStream} that delegates all operations to another {@link InputStream}.
   */
  private static final class DelegatingInputStream extends FilterInputStream {

    DelegatingInputStream(InputStream delegate) {
      super(delegate);
    }

    void setDelegate(InputStream delegate) {
      Closeables.closeQuietly(in);
      in = delegate;
    }
  }

  /**
   * An internal interface for processing payloads received from the
   * {@link #writeMessages(HttpRequest, HttpResponder, String, String, String, String, String, String, String)}
   * call.
   */
  private interface PayloadProcessor {

    /**
     * Process the given payload.
     *
     * @throws IOException if there is error when processing the payload
     * @throws BadRequestException if the request is invalid
     */
    void process(Iterator<byte[]> payloads) throws IOException, BadRequestException;
  }
}
