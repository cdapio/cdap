/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.master.environment.k8s;

import com.google.common.base.Preconditions;
import com.google.inject.Injector;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.messaging.Message;
import io.cdap.cdap.api.messaging.MessagingContext;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.internal.app.runtime.monitor.MonitorSchemas;
import io.cdap.cdap.internal.app.runtime.monitor.RuntimeMonitor;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.TopicMetadata;
import io.cdap.cdap.messaging.client.ClientMessagingService;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequestConfig;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Unit test for {@link RuntimeServiceMain}.
 */
public class RuntimeServiceMainTest extends MasterServiceMainTestBase {
    final protected String serviceName = Constants.Service.RUNTIME;

    @Test
    public void testPing() throws Exception {
        URL url = getRouterBaseURI().resolve("/ping").toURL();
        HttpResponse response = HttpRequests
                .execute(io.cdap.common.http.HttpRequest.get(url).build(),
                        new DefaultHttpRequestConfig(false));
        Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    }

    @Test
    public void testStatus() throws Exception {
        URL url = getRouterBaseURI().resolve(
                String.format("v3/system/services/%s/status", this.serviceName)).toURL();
        HttpResponse response = HttpRequests
                .execute(io.cdap.common.http.HttpRequest.get(url).build(),
                        new DefaultHttpRequestConfig(false));
        Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
        Assert.assertTrue(response.getResponseMessage().contains("OK"));
    }

    @Test
    public void testPostData() throws Exception {
        MessagingService messagingService = getMessgingService();
        MessagingContext messagingContext = new MultiThreadMessagingContext(messagingService);

        // Create a dummy topic
        String topic = "dummyTopic";
        createTopic(messagingService, topic);

        // Publish some messages to the dummy topic
        List<String> dummyMessages = Arrays.asList("dummyMessage1", "dummyMessage1");
        messagingContext.getDirectMessagePublisher().publish(
                NamespaceId.SYSTEM.getNamespace(), topic, StandardCharsets.UTF_8, dummyMessages.iterator());

        // Fetch the published dummy messages and posted as data to runtime service REST endpoint which will get published
        List<Message> messages = fetchMessages(messagingContext, topic);
        Map<String, String> topicConfigs = RuntimeMonitor.createTopicConfigs(cConf);
        String topicConfig = "audit.topic";
        Preconditions.checkNotNull(topicConfigs.get(topicConfig));
        Map<String, List<Message>> messagesForTopicConfig = new HashMap<String, List<Message>>() {{
            put(topicConfig, messages);
        }};
        OutputStream os = new ByteArrayOutputStream();
        encodeMessages(messagesForTopicConfig, os);
        URL url = getRouterBaseURI().resolve(
                String.format("v3/%s/data", this.serviceName)).toURL();
        HttpRequestConfig requestConfig = new HttpRequestConfig(0, 0, false);
        HttpResponse response = HttpRequests.execute(
                HttpRequest
                        .post(url)
                        .withBody(os.toString())
                        .build(), requestConfig);
        Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());


        // Fetch the data posted to runtime service
        List<String> published =
                fetchMessages(messagingContext, topicConfigs.get(topicConfig)).stream().map(
                        message -> message.getPayloadAsString()).collect(Collectors.toList());
        Assert.assertEquals(dummyMessages, published);
    }

    private MessagingService getMessgingService() {
        // Discover the TMS endpoint
        Injector injector = getServiceMainInstance(MessagingServiceMain.class).getInjector();
        DiscoveryServiceClient discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
        return new ClientMessagingService(discoveryServiceClient);
    }

    private void createTopic(MessagingService messagingService, String topic) throws Exception {
        TopicId topicId = NamespaceId.SYSTEM.topic(topic);
        messagingService.createTopic(new TopicMetadata(topicId));
    }

    private List<Message> fetchMessages(MessagingContext context, String topic) throws Exception {
        List<Message> messages = new ArrayList<Message>();
        CloseableIterator<Message> iterator = context.getMessageFetcher().fetch(
                NamespaceId.SYSTEM.getNamespace(), topic, Integer.MAX_VALUE, null);
        iterator.forEachRemaining(messages::add);
        return messages;
    }

    private void encodeMessages (Map<String, List<Message>> messagesForTopicConfig, OutputStream os) throws IOException {
        Encoder encoder = EncoderFactory.get().directBinaryEncoder(os, null);
        Schema elementSchema = MonitorSchemas.V1.MonitorResponse.SCHEMA.getValueType().getElementType();
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(elementSchema) {
            @Override
            protected void writeBytes(Object datum, Encoder out) throws IOException {
                if (datum instanceof byte[]) {
                    out.writeBytes((byte[]) datum);
                } else {
                    super.writeBytes(datum, out);
                }
            }
        };

        encoder.writeMapStart();
        encoder.setItemCount(messagesForTopicConfig.size());
        for (Map.Entry<String, List<Message>> entry : messagesForTopicConfig.entrySet()) {
            String topic = entry.getKey();
            List<Message> messages = entry.getValue();
            encoder.startItem();
            encoder.writeString(topic);
            encoder.writeArrayStart();
            encoder.setItemCount(messages.size());
            for (Message m : messages) {
                encoder.startItem();
                GenericRecord record = new GenericData.Record(elementSchema);
                record.put("messageId", m.getId());
                record.put("message", m.getPayload());
                encoder.startItem();
                writer.write(record, encoder);
            }
            encoder.writeArrayEnd();
        }
        encoder.writeMapEnd();
    }


}
