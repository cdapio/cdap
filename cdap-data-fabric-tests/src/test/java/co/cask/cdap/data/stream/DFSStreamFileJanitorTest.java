/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.data.stream;

import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.io.RootLocationFactory;
import co.cask.cdap.common.namespace.DefaultNamespacedLocationFactory;
import co.cask.cdap.common.namespace.InMemoryNamespaceClient;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.common.namespace.SimpleNamespaceQueryAdmin;
import co.cask.cdap.common.security.RemoteUGIProvider;
import co.cask.cdap.common.security.UGIProvider;
import co.cask.cdap.data.file.FileWriter;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.TransactionMetricsModule;
import co.cask.cdap.data.stream.service.InMemoryStreamMetaStore;
import co.cask.cdap.data.stream.service.StreamMetaStore;
import co.cask.cdap.data.view.ViewAdminModules;
import co.cask.cdap.data2.dataset2.InMemoryNamespaceStore;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.data2.metadata.store.NoOpMetadataStore;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.notifications.feeds.NotificationFeedManager;
import co.cask.cdap.notifications.feeds.service.NoOpNotificationFeedManager;
import co.cask.cdap.proto.Id;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.AuthorizationTestModule;
import co.cask.cdap.store.NamespaceStore;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.util.Modules;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.twill.filesystem.FileContextLocationFactory;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;

/**
 *
 */
public class DFSStreamFileJanitorTest extends StreamFileJanitorTestBase {

  private static LocationFactory locationFactory;
  private static NamespacedLocationFactory namespacedLocationFactory;
  private static StreamAdmin streamAdmin;
  private static MiniDFSCluster dfsCluster;
  private static StreamFileWriterFactory fileWriterFactory;
  private static StreamCoordinatorClient streamCoordinatorClient;
  private static NamespaceStore namespaceStore;
  private static NamespaceAdmin namespaceAdmin;

  @BeforeClass
  public static void init() throws IOException {

    Configuration hConf = new Configuration();
    hConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, tmpFolder.newFolder().getAbsolutePath());
    dfsCluster = new MiniDFSCluster.Builder(hConf).numDataNodes(1).build();
    dfsCluster.waitClusterUp();
    namespaceAdmin = new InMemoryNamespaceClient();
    final LocationFactory lf = new FileContextLocationFactory(dfsCluster.getFileSystem().getConf());
    final NamespacedLocationFactory nlf = new DefaultNamespacedLocationFactory(cConf, new RootLocationFactory(lf), lf,
                                                                               namespaceAdmin);

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new ZKClientModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(LocationFactory.class).toInstance(lf);
          bind(NamespacedLocationFactory.class).toInstance(nlf);
          bind(NamespaceAdmin.class).toInstance(namespaceAdmin);
          bind(NamespaceQueryAdmin.class).to(SimpleNamespaceQueryAdmin.class);
          bind(UGIProvider.class).to(RemoteUGIProvider.class);
        }
      },
      new TransactionMetricsModule(),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new DataFabricModules().getDistributedModules(),
      Modules.override(new DataSetsModules().getDistributedModules()).with(new AbstractModule() {
        @Override
        protected void configure() {
          bind(MetadataStore.class).to(NoOpMetadataStore.class);
        }
      }),
      new ExploreClientModule(),
      new ViewAdminModules().getInMemoryModules(),
      Modules.override(new StreamAdminModules().getDistributedModules()).with(new AbstractModule() {
        @Override
        protected void configure() {
          // Tests are running in same process, hence no need to have ZK to coordinate
          bind(StreamCoordinatorClient.class).to(InMemoryStreamCoordinatorClient.class).in(Scopes.SINGLETON);
          bind(StreamMetaStore.class).to(InMemoryStreamMetaStore.class);
        }
      }),
      new AbstractModule() {
        @Override
        protected void configure() {
          // We don't need notification in this test, hence inject an no-op one
          bind(NotificationFeedManager.class).to(NoOpNotificationFeedManager.class);
          bind(NamespaceStore.class).to(InMemoryNamespaceStore.class);
        }
      },
      new AuthorizationTestModule(),
      new AuthorizationEnforcementModule().getInMemoryModules(),
      new AuthenticationContextModules().getMasterModule()
    );

    locationFactory = injector.getInstance(LocationFactory.class);
    namespacedLocationFactory = injector.getInstance(NamespacedLocationFactory.class);
    namespaceStore = injector.getInstance(NamespaceStore.class);
    streamAdmin = injector.getInstance(StreamAdmin.class);
    fileWriterFactory = injector.getInstance(StreamFileWriterFactory.class);
    streamCoordinatorClient = injector.getInstance(StreamCoordinatorClient.class);
    streamCoordinatorClient.startAndWait();
  }

  @AfterClass
  public static void finish() {
    streamCoordinatorClient.stopAndWait();
    dfsCluster.shutdown();
  }

  @Override
  protected LocationFactory getLocationFactory() {
    return locationFactory;
  }

  @Override
  protected NamespacedLocationFactory getNamespacedLocationFactory() {
    return namespacedLocationFactory;
  }

  @Override
  protected StreamAdmin getStreamAdmin() {
    return streamAdmin;
  }

  @Override
  protected NamespaceStore getNamespaceStore() {
    return namespaceStore;
  }

  @Override
  protected NamespaceAdmin getNamespaceAdmin() {
    return namespaceAdmin;
  }

  @Override
  protected CConfiguration getCConfiguration() {
    return cConf;
  }

  @Override
  protected FileWriter<StreamEvent> createWriter(Id.Stream streamId) throws IOException {
    StreamConfig config = streamAdmin.getConfig(streamId);
    return fileWriterFactory.create(config, StreamUtils.getGeneration(config));
  }
}
