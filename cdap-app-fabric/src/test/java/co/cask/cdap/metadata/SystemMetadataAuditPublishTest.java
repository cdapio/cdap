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

package co.cask.cdap.metadata;

import co.cask.cdap.AllProgramsApp;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.data2.audit.AuditModule;
import co.cask.cdap.data2.audit.InMemoryAuditPublisher;
import co.cask.cdap.data2.metadata.store.DefaultMetadataStore;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.audit.AuditMessage;
import co.cask.cdap.proto.audit.AuditPayload;
import co.cask.cdap.proto.audit.AuditType;
import co.cask.cdap.proto.audit.payload.metadata.MetadataPayload;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.metadata.Metadata;
import co.cask.cdap.proto.metadata.MetadataScope;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tests for verifying that system metadata gets published to Kafka when publishing is enabled
 */
public class SystemMetadataAuditPublishTest {
  private static CConfiguration cConf;
  private static InMemoryAuditPublisher auditPublisher;
  private static NamespaceAdmin namespaceAdmin;

  @BeforeClass
  public static void setup() {
    cConf = CConfiguration.create();
    cConf.setBoolean(Constants.Audit.ENABLED, true);
    Injector injector = AppFabricTestHelper.getInjector(cConf, new AbstractModule() {
      @Override
      protected void configure() {
        bind(MetadataStore.class).to(DefaultMetadataStore.class);
        install(new AuditModule().getInMemoryModules());
      }
    });
    auditPublisher = injector.getInstance(InMemoryAuditPublisher.class);
    namespaceAdmin = injector.getInstance(NamespaceAdmin.class);
  }

  @Test
  public void testPublishing() throws Exception {
    AppFabricTestHelper.deployApplication(Id.Namespace.DEFAULT, AllProgramsApp.class, null, cConf);
    Set<String> addedMetadata = getAllSystemMetadata();
    Assert.assertFalse(addedMetadata.isEmpty());
    namespaceAdmin.delete(NamespaceId.DEFAULT);
    Set<String> removedMetadata = getAllSystemMetadata();
    Assert.assertFalse(removedMetadata.isEmpty());
    // Assert that the exact same system properties and tags got added upon app deployment and removed upon deletion
    Assert.assertEquals(addedMetadata, removedMetadata);
  }

  private Set<String> getAllSystemMetadata() {
    Set<String> allMetadata = new HashSet<>();
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
    return allMetadata;
  }

  private Iterable<AuditMessage> getMetadataUpdateMessages() {
    List<AuditMessage> auditMessages = auditPublisher.popMessages();
    return Iterables.filter(auditMessages, new Predicate<AuditMessage>() {
        @Override
        public boolean apply(AuditMessage input) {
          return AuditType.METADATA_CHANGE == input.getType();
        }
      }
    );
  }
}
