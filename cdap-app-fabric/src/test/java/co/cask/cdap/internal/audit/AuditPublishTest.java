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
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.messaging.TopicNotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.audit.AuditModule;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.data.RawMessage;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.audit.AuditMessage;
import co.cask.cdap.proto.audit.AuditType;
import co.cask.cdap.proto.codec.AuditMessageTypeAdapter;
import co.cask.cdap.proto.codec.EntityIdTypeAdapter;
import co.cask.cdap.proto.element.EntityType;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.Ids;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.TopicId;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Injector;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Tests audit publishing.
 */
public class AuditPublishTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(AuditMessage.class, new AuditMessageTypeAdapter())
    .registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())
    .create();

  private static CConfiguration cConf;
  private static MessagingService messagingService;
  private static TopicId auditTopic;

  @BeforeClass
  public static void init() throws Exception {
    cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());

    Injector injector = AppFabricTestHelper.getInjector(cConf, new AuditModule().getDistributedModules());
    messagingService = injector.getInstance(MessagingService.class);
    if (messagingService instanceof Service) {
      ((Service) messagingService).startAndWait();
    }

    auditTopic = NamespaceId.SYSTEM.topic(cConf.get(Constants.Audit.TOPIC));
  }

  @AfterClass
  public static void stop() throws Exception {
    if (messagingService instanceof Service) {
      ((Service) messagingService).stopAndWait();
    }
  }

  @Test
  public void testPublish() throws Exception {
    String defaultNs = NamespaceId.DEFAULT.getNamespace();
    String appName = WordCountApp.class.getSimpleName();

    // Define expected values
    Set<? extends EntityId> expectedMetadataChangeEntities =
      ImmutableSet.of(Ids.namespace(defaultNs).artifact(WordCountApp.class.getSimpleName(), "1"),
                      Ids.namespace(defaultNs).app(appName),
                      Ids.namespace(defaultNs).app(appName).flow(WordCountApp.WordCountFlow.class.getSimpleName()),
                      Ids.namespace(defaultNs).app(appName).mr(WordCountApp.VoidMapReduceJob.class.getSimpleName()),
                      Ids.namespace(defaultNs).app(appName)
                        .service(WordCountApp.WordFrequencyService.class.getSimpleName()),
                      Ids.namespace(defaultNs).dataset("mydataset"),
                      Ids.namespace(defaultNs).stream("text"));

    Multimap<AuditType, EntityId> expectedAuditEntities = HashMultimap.create();
    expectedAuditEntities.putAll(AuditType.METADATA_CHANGE, expectedMetadataChangeEntities);
    expectedAuditEntities.putAll(AuditType.CREATE, ImmutableSet.of(Ids.namespace(defaultNs).dataset("mydataset"),
                                                                   Ids.namespace(defaultNs).stream("text")));

    // Deploy application
    AppFabricTestHelper.deployApplication(Id.Namespace.DEFAULT, WordCountApp.class, null, cConf);

    // Verify audit messages
    List<AuditMessage> publishedMessages = fetchAuditMessages();

    Multimap<AuditType, EntityId> actualAuditEntities = HashMultimap.create();
    for (AuditMessage message : publishedMessages) {
      EntityId entityId = message.getEntityId();
      if (entityId instanceof NamespacedEntityId) {
        if (((NamespacedEntityId) entityId).getNamespace().equals(NamespaceId.SYSTEM.getNamespace())) {
          // Ignore system audit messages
          continue;
        }
      }
      if (entityId.getEntityType() == EntityType.ARTIFACT && entityId instanceof ArtifactId) {
        ArtifactId artifactId = (ArtifactId) entityId;
        // Version is dynamic for deploys in test cases
        entityId = Ids.namespace(artifactId.getNamespace()).artifact(artifactId.getArtifact(), "1");
      }
      actualAuditEntities.put(message.getType(), entityId);
    }
    Assert.assertEquals(expectedAuditEntities, actualAuditEntities);
  }

  private List<AuditMessage> fetchAuditMessages() throws TopicNotFoundException, IOException {
    List<AuditMessage> result = new ArrayList<>();
    try (CloseableIterator<RawMessage> iterator = messagingService.prepareFetch(auditTopic).fetch()) {
      while (iterator.hasNext()) {
        RawMessage message = iterator.next();
        result.add(GSON.fromJson(new String(message.getPayload(), StandardCharsets.UTF_8), AuditMessage.class));
      }
    }
    return result;
  }
}
