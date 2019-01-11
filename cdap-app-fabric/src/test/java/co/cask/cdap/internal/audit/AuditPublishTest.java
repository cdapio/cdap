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

package co.cask.cdap.internal.audit;

import co.cask.cdap.AllProgramsApp;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.app.ProgramType;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.messaging.TopicNotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.id.Id;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.data2.audit.AuditModule;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.internal.app.deploy.Specifications;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.data.RawMessage;
import co.cask.cdap.proto.audit.AuditMessage;
import co.cask.cdap.proto.audit.AuditType;
import co.cask.cdap.proto.codec.AuditMessageTypeAdapter;
import co.cask.cdap.proto.codec.EntityIdTypeAdapter;
import co.cask.cdap.proto.element.EntityType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.Ids;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.TopicId;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Injector;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
  public static void stop() {
    if (messagingService instanceof Service) {
      ((Service) messagingService).stopAndWait();
    }
  }

  @Test
  public void testPublish() throws Exception {
    String appName = AllProgramsApp.NAME;
    ApplicationId appId = NamespaceId.DEFAULT.app(appName);

    ApplicationSpecification spec = Specifications.from(new AllProgramsApp());

    // Define expected values
    Set<EntityId> expectedMetadataChangeEntities = new HashSet<>();

    // Metadata change on the artifact and app
    expectedMetadataChangeEntities.add(NamespaceId.DEFAULT.artifact(AllProgramsApp.class.getSimpleName(), "1"));
    expectedMetadataChangeEntities.add(appId);

    // All programs would have metadata change
    for (ProgramType programType : ProgramType.values()) {
      for (String programName : spec.getProgramsByType(programType)) {
        co.cask.cdap.proto.ProgramType internalProgramType = co.cask.cdap.proto.ProgramType.valueOf(programType.name());
        expectedMetadataChangeEntities.add(appId.program(internalProgramType, programName));
      }
    }

    // All store would have metadata change as well as creation
    Set<EntityId> expectedCreateEntities = new HashSet<>();
    for (String dataset : spec.getDatasets().keySet()) {
      expectedCreateEntities.add(NamespaceId.DEFAULT.dataset(dataset));
    }
    expectedMetadataChangeEntities.addAll(expectedCreateEntities);

    // TODO (CDAP-14733): Scheduler doesn't publish CREATE audit events. Once it does, we must expect them here, too.
    for (String schedule: spec.getProgramSchedules().keySet()) {
      expectedMetadataChangeEntities.add(appId.schedule(schedule));
    }

    // Stream only has creation (eventually will be removed)
    for (String stream : spec.getStreams().keySet()) {
      expectedCreateEntities.add(NamespaceId.DEFAULT.stream(stream));
    }

    Multimap<AuditType, EntityId> expectedAuditEntities = HashMultimap.create();
    expectedAuditEntities.putAll(AuditType.METADATA_CHANGE, expectedMetadataChangeEntities);

    expectedAuditEntities.putAll(AuditType.CREATE, expectedCreateEntities);

    // Deploy application
    AppFabricTestHelper.deployApplication(Id.Namespace.DEFAULT, AllProgramsApp.class, null, cConf);

    // Verify audit messages
    Tasks.waitFor(expectedAuditEntities, () -> {
      List<AuditMessage> publishedMessages = fetchAuditMessages();
      Multimap<AuditType, EntityId> actualAuditEntities = HashMultimap.create();
      for (AuditMessage message : publishedMessages) {
        EntityId entityId = EntityId.fromMetadataEntity(message.getEntity());
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
      return actualAuditEntities;
    }, 5, TimeUnit.SECONDS);
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
