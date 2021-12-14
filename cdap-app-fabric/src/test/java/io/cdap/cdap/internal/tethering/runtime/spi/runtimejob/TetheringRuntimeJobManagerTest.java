/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.internal.tethering.runtime.spi.runtimejob;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.messaging.Message;
import io.cdap.cdap.api.messaging.MessageFetcher;
import io.cdap.cdap.api.messaging.TopicAlreadyExistsException;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.internal.tethering.TetheringControlMessage;
import io.cdap.cdap.internal.tethering.runtime.spi.provisioner.TetheringConf;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.TopicMetadata;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.cdap.messaging.guice.MessagingServerRuntimeModule;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

public class TetheringRuntimeJobManagerTest {

  private static final Gson GSON = new GsonBuilder().create();
  private static final String TETHERED_INSTANCE_NAME = "other-instance";
  private static final String TETHERED_NAMESPACE_NAME = "other-namespace";
  private static final Map<String, String> PROPERTIES = ImmutableMap.of(TetheringConf.TETHERED_INSTANCE_PROPERTY,
                                                                        TETHERED_INSTANCE_NAME,
                                                                        TetheringConf.TETHERED_NAMESPACE_PROPERTY,
                                                                        TETHERED_NAMESPACE_NAME);

  private static MessageFetcher messageFetcher;
  private static MessagingService messagingService;
  private static TetheringRuntimeJobManager runtimeJobManager;
  private static TopicId topicId;

  @BeforeClass
  public static void setUp() throws IOException, TopicAlreadyExistsException {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Tethering.TOPIC_PREFIX, "prefix-");
    Injector injector = Guice.createInjector(
      new ConfigModule(cConf),
      new InMemoryDiscoveryModule(),
      new MessagingServerRuntimeModule().getInMemoryModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class).in(Scopes.SINGLETON);
        }
      });
    messagingService = injector.getInstance(MessagingService.class);
    if (messagingService instanceof Service) {
      ((Service) messagingService).startAndWait();
    }

    TetheringConf conf = TetheringConf.fromProperties(PROPERTIES);
    topicId = new TopicId(NamespaceId.SYSTEM.getNamespace(),
                          cConf.get(Constants.Tethering.TOPIC_PREFIX) + TETHERED_INSTANCE_NAME);
    messagingService.createTopic(new TopicMetadata(topicId, Collections.emptyMap()));
    messageFetcher = new MultiThreadMessagingContext(messagingService).getMessageFetcher();
    runtimeJobManager = new TetheringRuntimeJobManager(conf, cConf, messagingService);
  }

  @AfterClass
  public static void tearDown() throws TopicNotFoundException, IOException {
    messagingService.deleteTopic(topicId);
    if (messagingService instanceof Service) {
      ((Service) messagingService).stopAndWait();
    }
  }

  @Test
  public void testPublishToControlChannel() throws Exception {
    TetheringControlMessage message = new TetheringControlMessage(TetheringControlMessage.Type.RUN_PIPELINE,
                                                                  "payload".getBytes(StandardCharsets.UTF_8));
    runtimeJobManager.publishToControlChannel(message);
    try (CloseableIterator<Message> iterator = messageFetcher.fetch(topicId.getNamespace(), topicId.getTopic(), 1, 0)) {
      Assert.assertTrue(iterator.hasNext());
      Assert.assertEquals(GSON.toJson(message), iterator.next().getPayloadAsString());
    }
  }
}
