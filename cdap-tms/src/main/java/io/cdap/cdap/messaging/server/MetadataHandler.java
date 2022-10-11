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

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.TopicMetadata;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * A netty http handler for metadata REST API for the messaging system.
 */
@Path("/v1/namespaces/{namespace}")
public final class MetadataHandler extends AbstractHttpHandler {

  private static final Gson GSON = new Gson();
  private static final Type TOPIC_PROPERTY_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Type TOPIC_LIST_TYPE = new TypeToken<List<String>>() { }.getType();
  private static final Function<TopicId, String> TOPIC_TO_NAME = new Function<TopicId, String>() {
    @Override
    public String apply(TopicId topicId) {
      return topicId.getTopic();
    }
  };

  private final MessagingService messagingService;

  @Inject
  MetadataHandler(MessagingService messagingService) {
    this.messagingService = messagingService;
  }

  @PUT
  @Path("/topics/{topic}")
  public void createTopic(FullHttpRequest request, HttpResponder responder,
                          @PathParam("namespace") String namespace,
                          @PathParam("topic") String topic) throws Exception {
    TopicId topicId = new NamespaceId(namespace).topic(topic);
    messagingService.createTopic(new TopicMetadata(topicId.toSpiTopicId(), decodeTopicProperties(request.content())));
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @PUT
  @Path("/topics/{topic}/properties")
  public void updateTopic(FullHttpRequest request, HttpResponder responder,
                          @PathParam("namespace") String namespace,
                          @PathParam("topic") String topic) throws Exception {
    TopicId topicId = new NamespaceId(namespace).topic(topic);
    messagingService.updateTopic(new TopicMetadata(topicId.toSpiTopicId(), decodeTopicProperties(request.content())));
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @GET
  @Path("/topics/{topic}")
  public void getTopic(HttpRequest request, HttpResponder responder,
                       @PathParam("namespace") String namespace,
                       @PathParam("topic") String topic) throws Exception {
    TopicId topicId = new NamespaceId(namespace).topic(topic);
    TopicMetadata metadata = messagingService.getTopic(topicId);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(metadata.getProperties(), TOPIC_PROPERTY_TYPE));
  }

  @GET
  @Path("/topics")
  public void listTopics(HttpRequest request, HttpResponder responder,
                         @PathParam("namespace") String namespace) throws Exception {
    responder.sendJson(HttpResponseStatus.OK,
                       GSON.toJson(
                         Lists.transform(messagingService.listTopics(new NamespaceId(namespace)), TOPIC_TO_NAME),
                         TOPIC_LIST_TYPE));
  }

  @DELETE
  @Path("/topics/{topic}")
  public void deleteTopic(HttpRequest request, HttpResponder responder,
                          @PathParam("namespace") String namespace,
                          @PathParam("topic") String topic) throws Exception {
    TopicId topicId = new NamespaceId(namespace).topic(topic);
    messagingService.deleteTopic(topicId);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Decodes the topic property map from the given request body.
   */
  private Map<String, String> decodeTopicProperties(ByteBuf channelBuffer) throws BadRequestException {
    if (!channelBuffer.isReadable()) {
      return Collections.emptyMap();
    }

    try {
      return GSON.fromJson(new InputStreamReader(new ByteBufInputStream(channelBuffer), StandardCharsets.UTF_8),
                           TOPIC_PROPERTY_TYPE);
    } catch (Exception e) {
      throw new BadRequestException("Invalid topic properties. It must be JSON object with string values.");
    }
  }
}
