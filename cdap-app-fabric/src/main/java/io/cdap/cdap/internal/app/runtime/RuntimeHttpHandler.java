/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime;

import com.google.inject.Inject;
import io.cdap.cdap.api.messaging.MessagingContext;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.runtime.monitor.MonitorConsumeRequest;
import io.cdap.cdap.internal.app.runtime.monitor.MonitorMessage;
import io.cdap.cdap.internal.app.runtime.monitor.MonitorSchemas;
import io.cdap.cdap.internal.app.runtime.monitor.RemoteExecutionLogProcessor;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.*;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * HttpHandler for Metadata
 */
@Path(Constants.Gateway.API_VERSION_3)
public class RuntimeHttpHandler extends AbstractHttpHandler {
    private static final Logger LOG = LoggerFactory.getLogger(RuntimeHttpHandler.class);

    private final CConfiguration cConf;
    private final DatumReader<GenericRecord> responseDatumReader;
    private final RuntimePublisher publisher;
    private final MessagingContext messagingContext;


    @Inject
    RuntimeHttpHandler(CConfiguration cConf, MessagingService messagingService, RemoteExecutionLogProcessor logProcessor) {
        this.cConf = cConf;
        this.responseDatumReader = new GenericDatumReader<>(
                MonitorSchemas.V1.MonitorResponse.SCHEMA.getValueType().getElementType());
        this.messagingContext = new MultiThreadMessagingContext(messagingService);
        this.publisher = new RuntimePublisher(cConf, messagingContext,logProcessor);
    }

    @GET
    @Path("/runtime/hi")
    public void hi(HttpRequest request, HttpResponder responder) {
        responder.sendStatus(HttpResponseStatus.OK);
    }

    @POST
    @Path("/runtime/data")
    public void addMetadata(FullHttpRequest request, HttpResponder responder) throws Exception{
        Map<String, Deque<MonitorMessage>> messageMap = decodeMetadata(new ByteBufInputStream(request.content()));
        publisher.publishMetadata(messageMap);
        Map<String, MonitorConsumeRequest> result = new HashMap<>();
        int limit = cConf.getInt(Constants.RuntimeMonitor.BATCH_SIZE);
        for (Map.Entry<String, Deque<MonitorMessage>> entry : messageMap.entrySet()) {
            String topicConfig = entry.getKey();
            MonitorMessage lastMessage = entry.getValue().getLast();
            result.put(topicConfig, new MonitorConsumeRequest(lastMessage.getMessageId(), limit));
        }
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        encodeResponse(result, os);
        responder.sendByteArray(HttpResponseStatus.OK, os.toByteArray(), new DefaultHttpHeaders().set(HttpHeaderNames.CONTENT_TYPE, "avro/binary"));
    }

    private void encodeResponse(Map<String, MonitorConsumeRequest> topicsToRequest,
                               OutputStream outputStream) throws IOException {
        Encoder encoder = EncoderFactory.get().directBinaryEncoder(outputStream, null);
        encoder.writeMapStart();
        encoder.setItemCount(topicsToRequest.size());

        DatumWriter<GenericRecord> requestDatumWriter = new GenericDatumWriter<>(
                MonitorSchemas.V1.MonitorConsumeRequest.SCHEMA.getValueType());

        for (Map.Entry<String, MonitorConsumeRequest> requestEntry : topicsToRequest.entrySet()) {
            encoder.startItem();
            encoder.writeString(requestEntry.getKey());
            requestDatumWriter.write(requestEntry.getValue().toGenericRecord(), encoder);
        }

        encoder.writeMapEnd();
    }


    private Map<String, Deque<MonitorMessage>> decodeMetadata(InputStream is) {
        Decoder decoder = DecoderFactory.get().directBinaryDecoder(is, null);

        Map<String, Deque<MonitorMessage>> decodedMessages = new HashMap<>();

        try {
            long entries = decoder.readMapStart();
            while (entries > 0) {
                for (int i = 0; i < entries; i++) {
                    String topicConfig = decoder.readString();
                    if (topicConfig.isEmpty()) {
                        continue;
                    }
                    decodedMessages.put(topicConfig, decodeMessages(decoder));
                }

                entries = decoder.mapNext();
            }
        } catch (IOException e) {
            // catch the exception to process all the decoded messages to avoid refetching them.
            LOG.error("Error while decoding response from Runtime Server. ", e);
        }
        return decodedMessages;
    }

    private Deque<MonitorMessage> decodeMessages(Decoder decoder) throws IOException {
        Deque<MonitorMessage> decodedMessages = new LinkedList<>();
        long messages = decoder.readArrayStart();
        while (messages > 0) {
            GenericRecord reuse = new GenericData.Record(MonitorSchemas.V1.MonitorResponse.SCHEMA.getValueType()
                    .getElementType());
            for (int j = 0; j < messages; j++) {
                reuse = responseDatumReader.read(reuse, decoder);
                decodedMessages.add(new MonitorMessage(reuse));
            }

            messages = decoder.arrayNext();
        }

        return decodedMessages;
    }
}
