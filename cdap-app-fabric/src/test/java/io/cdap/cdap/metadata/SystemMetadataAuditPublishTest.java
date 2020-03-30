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

package io.cdap.cdap.metadata;

import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import io.cdap.cdap.AllProgramsApp;
import io.cdap.cdap.api.metadata.Metadata;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.data2.audit.AuditTestModule;
import io.cdap.cdap.data2.audit.InMemoryAuditPublisher;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.services.AppFabricServer;
import io.cdap.cdap.proto.audit.AuditMessage;
import io.cdap.cdap.proto.audit.AuditPayload;
import io.cdap.cdap.proto.audit.AuditType;
import io.cdap.cdap.proto.audit.payload.metadata.MetadataPayload;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.scheduler.Scheduler;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Tests for verifying that system metadata gets published to Kafka when publishing is enabled
 */
public class SystemMetadataAuditPublishTest {
  private static CConfiguration cConf;
  private static InMemoryAuditPublisher auditPublisher;
  private static NamespaceAdmin namespaceAdmin;
  private static Scheduler scheduler;
  private static AppFabricServer appFabricServer;

  @BeforeClass
  public static void setup() {
    cConf = CConfiguration.create();
    cConf.setBoolean(Constants.Audit.ENABLED, true);
    Injector injector = AppFabricTestHelper.getInjector(cConf, new AbstractModule() {
      @Override
      protected void configure() {
        install(new AuditTestModule());
      }
    });
    auditPublisher = injector.getInstance(InMemoryAuditPublisher.class);
    namespaceAdmin = injector.getInstance(NamespaceAdmin.class);
    scheduler = injector.getInstance(Scheduler.class);
    if (scheduler instanceof Service) {
      ((Service) scheduler).startAndWait();
    }
    appFabricServer = injector.getInstance(AppFabricServer.class);
    appFabricServer.startAndWait();
  }

  @AfterClass
  public static void tearDown() {
    appFabricServer.stopAndWait();
    if (scheduler instanceof Service) {
      ((Service) scheduler).stopAndWait();
    }
    AppFabricTestHelper.shutdown();
  }

  @Test
  public void testPublishing() throws Exception {
    AppFabricTestHelper.deployApplication(Id.Namespace.DEFAULT, AllProgramsApp.class, null, cConf);
    Set<String> addedMetadata = new HashSet<>();
    // TODO (CDAP-14670): this test is brittle, find a better condition to wait on
    Tasks.waitFor(26, () -> addAllSystemMetadata(addedMetadata), 10, TimeUnit.SECONDS);
    namespaceAdmin.delete(NamespaceId.DEFAULT);
    Set<String> removedMetadata = new HashSet<>();
    // expect the same number of changes when namespace is deleted
    Tasks.waitFor(addedMetadata.size(), () -> addAllSystemMetadata(removedMetadata), 5, TimeUnit.SECONDS);
    // Assert that the exact same system properties and tags got added upon app deployment and removed upon deletion
    Assert.assertEquals(addedMetadata, removedMetadata);
  }

  private int addAllSystemMetadata(Set<String> allMetadata) {
    for (AuditMessage auditMessage : getMetadataUpdateMessages()) {
      AuditPayload payload = auditMessage.getPayload();
      Assert.assertTrue(payload instanceof MetadataPayload);
      MetadataPayload metadataPayload = (MetadataPayload) payload;
      Map<MetadataScope, Metadata> additions = metadataPayload.getAdditions();
      if (additions.containsKey(MetadataScope.SYSTEM)) {
        allMetadata.addAll(additions.get(MetadataScope.SYSTEM).getProperties().keySet());
        allMetadata.addAll(additions.get(MetadataScope.SYSTEM).getTags());
      }
      Map<MetadataScope, Metadata> deletions = metadataPayload.getDeletions();
      if (deletions.containsKey(MetadataScope.SYSTEM)) {
        allMetadata.addAll(deletions.get(MetadataScope.SYSTEM).getProperties().keySet());
        allMetadata.addAll(deletions.get(MetadataScope.SYSTEM).getTags());
      }
    }
    return allMetadata.size();
  }

  private Iterable<AuditMessage> getMetadataUpdateMessages() {
    List<AuditMessage> auditMessages = auditPublisher.popMessages();
    return auditMessages.stream()
      .filter(input -> AuditType.METADATA_CHANGE == input.getType())
      .collect(Collectors.toList());
  }
}
