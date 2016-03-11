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

package co.cask.cdap.internal.audit;

import co.cask.cdap.WordCountApp;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.KafkaClientModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.data2.audit.AuditModule;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.kafka.KafkaTester;
import co.cask.cdap.proto.audit.AuditMessage;
import co.cask.cdap.proto.audit.AuditType;
import co.cask.cdap.proto.codec.AuditMessageTypeAdapter;
import co.cask.cdap.proto.codec.EntityIdTypeAdapter;
import co.cask.cdap.proto.element.EntityType;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.Ids;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedArtifactId;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Tests audit publishing.
 */
public class AuditPublishTest {
  @ClassRule
  public static final KafkaTester KAFKA_TESTER = new KafkaTester(
    ImmutableMap.of(Constants.Audit.ENABLED, "true"), Collections.<Module>emptyList(), 1);

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(AuditMessage.class, new AuditMessageTypeAdapter())
    .registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())
    .create();

  private static ZKClientService zkClient;
  private static KafkaClientService kafkaClient;

  @BeforeClass
  public static void init() throws Exception {
    Injector injector = AppFabricTestHelper.getInjector(KAFKA_TESTER.getCConf(),
                                                        new AbstractModule() {
                                                          @Override
                                                          protected void configure() {
                                                            install(new AuditModule().getDistributedModules());
                                                            install(new ZKClientModule());
                                                            install(new KafkaClientModule());
                                                          }
                                                        });

    zkClient = injector.getInstance(ZKClientService.class);
    zkClient.startAndWait();
    kafkaClient = injector.getInstance(KafkaClientService.class);
    kafkaClient.startAndWait();
  }

  @AfterClass
  public static void stop() throws Exception {
    zkClient.stopAndWait();
    kafkaClient.startAndWait();
  }

  @Test
  public void testPublish() throws Exception {
    String defaultNs = NamespaceId.DEFAULT.getNamespace();
    String appName = WordCountApp.class.getSimpleName();
    Set<? extends EntityId> expectedMetadataChangeEntities =
      ImmutableSet.of(Ids.namespace(defaultNs).artifact("app", "1"),
                      Ids.namespace(defaultNs).app(appName),
                      Ids.namespace(defaultNs).app(appName).flow(WordCountApp.WordCountFlow.class.getSimpleName()),
                      Ids.namespace(defaultNs).app(appName).mr(WordCountApp.VoidMapReduceJob.class.getSimpleName()),
                      Ids.namespace(defaultNs).app(appName)
                        .service(WordCountApp.WordFrequencyService.class.getSimpleName()),
                      Ids.namespace(defaultNs).dataset("mydataset"),
                      Ids.namespace(defaultNs).stream("text"));

    Multimap<AuditType, EntityId> expectedAuditEntities = HashMultimap.create();
    expectedAuditEntities.putAll(AuditType.METADATA_CHANGE, expectedMetadataChangeEntities);
    expectedAuditEntities.put(AuditType.CREATE, Ids.namespace(defaultNs).stream("text"));

    AppFabricTestHelper.deployApplication(WordCountApp.class);
    List<AuditMessage> publishedMessages =
      KAFKA_TESTER.getPublishedMessages(KAFKA_TESTER.getCConf().get(Constants.Audit.KAFKA_TOPIC), 11,
                                        AuditMessage.class, GSON);

    Multimap<AuditType, EntityId> actualAuditEntities = HashMultimap.create();
    for (AuditMessage message : publishedMessages) {
      EntityId entityId = message.getEntityId();
      if (entityId.getEntity() == EntityType.ARTIFACT) {
        NamespacedArtifactId artifactId = (NamespacedArtifactId) entityId;
        // Version is dynamic for deploys in test cases
        entityId = Ids.namespace(artifactId.getNamespace()).artifact(artifactId.getArtifact(), "1");
      }
      actualAuditEntities.put(message.getType(), entityId);
    }
    Assert.assertEquals(expectedAuditEntities, actualAuditEntities);
  }
}
