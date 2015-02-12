/*
 * Copyright © 2014 Cask Data, Inc.
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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.TransactionMetricsModule;
import co.cask.cdap.data.stream.service.InMemoryStreamMetaStore;
import co.cask.cdap.data.stream.service.StreamMetaStore;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.notifications.feeds.guice.NotificationFeedServiceRuntimeModule;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.twill.filesystem.HDFSLocationFactory;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;

/**
 * Tests for {@link DistributedStreamCoordinatorClient}
 */
public class DistributedStreamCoordinatorClientTest extends StreamCoordinatorTestBase {

  private static InMemoryZKServer zkServer;
  private static ZKClientService zkClient;
  private static MiniDFSCluster dfsCluster;
  private static StreamAdmin streamAdmin;
  private static StreamCoordinatorClient coordinatorClient;

  @BeforeClass
  public static void init() throws IOException {
    zkServer = InMemoryZKServer.builder().setDataDir(tmpFolder.newFolder()).build();
    zkServer.startAndWait();

    Configuration hConf = new Configuration();
    hConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, tmpFolder.newFolder().getAbsolutePath());
    dfsCluster = new MiniDFSCluster.Builder(hConf).numDataNodes(1).build();
    final FileSystem fileSystem = dfsCluster.getFileSystem();

    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Zookeeper.QUORUM, zkServer.getConnectionStr());

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf),
      new ZKClientModule(),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new DataFabricModules().getDistributedModules(),
      new DataSetsModules().getDistributedModule(),
      new TransactionMetricsModule(),
      new NotificationFeedServiceRuntimeModule().getInMemoryModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(LocationFactory.class).toInstance(new HDFSLocationFactory(fileSystem));
        }
      },
      Modules.override(new StreamAdminModules().getDistributedModules())
        .with(new AbstractModule() {
          @Override
          protected void configure() {
            bind(StreamMetaStore.class).to(InMemoryStreamMetaStore.class);
          }
        })
    );

    zkClient = injector.getInstance(ZKClientService.class);
    zkClient.startAndWait();

    streamAdmin = injector.getInstance(StreamAdmin.class);
    coordinatorClient = injector.getInstance(StreamCoordinatorClient.class);
    coordinatorClient.startAndWait();
  }

  @AfterClass
  public static void finish() {
    coordinatorClient.stopAndWait();
    dfsCluster.shutdown();
    zkClient.stopAndWait();
    zkServer.stopAndWait();
  }

  @Override
  protected StreamCoordinatorClient getStreamCoordinator() {
    return coordinatorClient;
  }

  @Override
  protected StreamAdmin getStreamAdmin() {
    return streamAdmin;
  }
}
