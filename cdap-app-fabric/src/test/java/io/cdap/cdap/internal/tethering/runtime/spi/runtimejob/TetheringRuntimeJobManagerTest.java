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
import io.cdap.cdap.common.guice.LocalLocationModule;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.data.runtime.StorageModule;
import io.cdap.cdap.data.runtime.SystemDatasetRuntimeModule;
import io.cdap.cdap.internal.tethering.NamespaceAllocation;
import io.cdap.cdap.internal.tethering.PeerAlreadyExistsException;
import io.cdap.cdap.internal.tethering.PeerInfo;
import io.cdap.cdap.internal.tethering.PeerMetadata;
import io.cdap.cdap.internal.tethering.TetheringControlMessage;
import io.cdap.cdap.internal.tethering.TetheringStatus;
import io.cdap.cdap.internal.tethering.TetheringStore;
import io.cdap.cdap.internal.tethering.runtime.spi.provisioner.TetheringConf;
import io.cdap.cdap.internal.tethering.runtime.spi.provisioner.TetheringProvisioner;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.TopicMetadata;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.cdap.messaging.guice.MessagingServerRuntimeModule;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.store.StoreDefinition;
import org.apache.commons.io.IOUtils;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.runtime.TransactionModules;
import org.apache.twill.api.LocalFile;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.DefaultLocalFile;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public class TetheringRuntimeJobManagerTest {

  private static final Gson GSON = new GsonBuilder().create();
  private static final String TETHERED_INSTANCE_NAME = "other-instance";
  private static final String TETHERED_NAMESPACE_NAME = "other_namespace";
  private static final Map<String, String> PROPERTIES = ImmutableMap.of(TetheringConf.TETHERED_INSTANCE_PROPERTY,
                                                                        TETHERED_INSTANCE_NAME,
                                                                        TetheringConf.TETHERED_NAMESPACE_PROPERTY,
                                                                        TETHERED_NAMESPACE_NAME);

  private static MessageFetcher messageFetcher;
  private static MessagingService messagingService;
  private static TetheringRuntimeJobManager runtimeJobManager;
  private static TopicId topicId;
  private static TransactionManager txManager;
  private static TetheringStore tetheringStore;
  private static TetheringProvisioner tetheringProvisioner;

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @BeforeClass
  public static void setUp() throws IOException, TopicAlreadyExistsException, PeerAlreadyExistsException {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Tethering.CLIENT_TOPIC_PREFIX, "prefix-");
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    Injector injector = Guice.createInjector(
      new ConfigModule(cConf),
      new InMemoryDiscoveryModule(),
      new LocalLocationModule(),
      new AuthorizationEnforcementModule().getNoOpModules(),
      new MessagingServerRuntimeModule().getInMemoryModules(),
      new SystemDatasetRuntimeModule().getInMemoryModules(),
      new TransactionModules().getInMemoryModules(),
      new StorageModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class).in(Scopes.SINGLETON);
        }
      });
    // Define all StructuredTable before starting any services that need StructuredTable
    StoreDefinition.createAllTables(injector.getInstance(StructuredTableAdmin.class));
    messagingService = injector.getInstance(MessagingService.class);
    if (messagingService instanceof Service) {
      ((Service) messagingService).startAndWait();
    }

    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();
    tetheringStore = injector.getInstance(TetheringStore.class);
    PeerMetadata metadata = new PeerMetadata(Collections.singletonList(new NamespaceAllocation(TETHERED_NAMESPACE_NAME,
                                                                                               null,
                                                                                               null)),
                                             Collections.emptyMap(), null);
    PeerInfo peerInfo = new PeerInfo(TETHERED_INSTANCE_NAME, null, TetheringStatus.ACCEPTED,
                                     metadata, System.currentTimeMillis());
    tetheringStore.addPeer(peerInfo);
    TetheringConf conf = TetheringConf.fromProperties(PROPERTIES);
    topicId = new TopicId(NamespaceId.SYSTEM.getNamespace(),
                          cConf.get(Constants.Tethering.CLIENT_TOPIC_PREFIX) + TETHERED_INSTANCE_NAME);
    messagingService.createTopic(new TopicMetadata(topicId, Collections.emptyMap()));
    messageFetcher = new MultiThreadMessagingContext(messagingService).getMessageFetcher();
    runtimeJobManager = new TetheringRuntimeJobManager(conf, cConf, messagingService, tetheringStore,
                                                       injector.getInstance(LocationFactory.class));
    tetheringProvisioner = injector.getInstance(TetheringProvisioner.class);
  }

  @AfterClass
  public static void tearDown() throws TopicNotFoundException, IOException {
    if (txManager != null) {
      txManager.stopAndWait();
    }
    messagingService.deleteTopic(topicId);
    if (messagingService instanceof Service) {
      ((Service) messagingService).stopAndWait();
    }
  }

  @Test
  public void testPublishToControlChannel() throws Exception {
    TetheringControlMessage message = new TetheringControlMessage(TetheringControlMessage.Type.START_PROGRAM,
                                                                  "payload".getBytes(StandardCharsets.UTF_8));
    runtimeJobManager.publishToControlChannel(message);
    try (CloseableIterator<Message> iterator = messageFetcher.fetch(topicId.getNamespace(), topicId.getTopic(), 1, 0)) {
      Assert.assertTrue(iterator.hasNext());
      Assert.assertEquals(GSON.toJson(message), iterator.next().getPayloadAsString());
    }
  }

  @Test
  public void testGetLocalFileAsCompressedString() throws IOException {
    File file = File.createTempFile("test", "xml");
    file.deleteOnExit();
    String fileContents = "contents of test.xml";
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
      writer.write(fileContents);
    }
    LocalFile localfile = new DefaultLocalFile(file.getName(), file.toURI(), file.lastModified(), file.length(),
                                               false, null);
    byte[] compressedContents = runtimeJobManager.getLocalFileAsCompressedBytes(localfile);

    // test that uncompressed contents matches original file contents
    String uncompressedContents;
    try (GZIPInputStream inputStream = new GZIPInputStream(new ByteArrayInputStream(compressedContents))) {
      uncompressedContents = IOUtils.toString(inputStream);
    }
    Assert.assertEquals(fileContents, uncompressedContents);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidateInvalidNamespace() {
    Map<String, String> properties = ImmutableMap.of(TetheringConf.TETHERED_INSTANCE_PROPERTY,
                                                     "my-instance",
                                                     TetheringConf.TETHERED_NAMESPACE_PROPERTY,
                                                     "invalid-ns");
    // Validation should fail as namespace can only contain alphanumeric characters or _
    tetheringProvisioner.validateProperties(properties);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidateNamespaceNotFound() {
    // Validation should fail because default namespace is not associated with the tethering
    runtimeJobManager.checkTetheredConnection(TETHERED_INSTANCE_NAME, "default");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidatePeerNotFound() {
    // Validation should fail because the peer is not tethered
    runtimeJobManager.checkTetheredConnection("unknown_peer", TETHERED_INSTANCE_NAME);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidateConnectionStatus() throws IOException {
    tetheringStore.updatePeerStatusAndTimestamp(TETHERED_INSTANCE_NAME, TetheringStatus.PENDING);
    // Validation should fail because the tethering status is not yet accepted
    runtimeJobManager.checkTetheredConnection(TETHERED_INSTANCE_NAME, TETHERED_INSTANCE_NAME);
  }
}
