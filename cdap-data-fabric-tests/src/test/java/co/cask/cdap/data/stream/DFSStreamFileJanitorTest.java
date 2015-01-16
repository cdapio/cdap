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

import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.data.file.FileWriter;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.TransactionMetricsModule;
import co.cask.cdap.data.stream.service.InMemoryStreamMetaStore;
import co.cask.cdap.data.stream.service.StreamMetaStore;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.notifications.feeds.guice.NotificationFeedServiceRuntimeModule;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.util.Modules;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.twill.filesystem.HDFSLocationFactory;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;

/**
 *
 */
public class DFSStreamFileJanitorTest extends StreamFileJanitorTestBase {

  private static CConfiguration cConf;
  private static LocationFactory locationFactory;
  private static StreamAdmin streamAdmin;
  private static MiniDFSCluster dfsCluster;
  private static StreamFileWriterFactory fileWriterFactory;
  private static StreamCoordinator streamCoordinator;

  @BeforeClass
  public static void init() throws IOException {
    Configuration hConf = new Configuration();
    hConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, tmpFolder.newFolder().getAbsolutePath());
    dfsCluster = new MiniDFSCluster.Builder(hConf).numDataNodes(1).build();
    final FileSystem fileSystem = dfsCluster.getFileSystem();

    cConf = CConfiguration.create();

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new ZKClientModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(LocationFactory.class).toInstance(new HDFSLocationFactory(fileSystem));
        }
      },
      new TransactionMetricsModule(),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new DataFabricModules().getDistributedModules(),
      Modules.override(new StreamAdminModules().getDistributedModules()).with(new AbstractModule() {

        @Override
        protected void configure() {
          // Tests are running in same process, hence no need to have ZK to coordinate
          bind(StreamCoordinator.class).to(InMemoryStreamCoordinator.class).in(Scopes.SINGLETON);
          bind(StreamAdmin.class).to(TestStreamFileAdmin.class).in(Scopes.SINGLETON);
          bind(StreamMetaStore.class).to(InMemoryStreamMetaStore.class);
        }
      }),
      new DataSetsModules().getDistributedModule(),
      new NotificationFeedServiceRuntimeModule().getInMemoryModules()
    );

    locationFactory = injector.getInstance(LocationFactory.class);
    streamAdmin = injector.getInstance(StreamAdmin.class);
    fileWriterFactory = injector.getInstance(StreamFileWriterFactory.class);
    streamCoordinator = injector.getInstance(StreamCoordinator.class);
    streamCoordinator.startAndWait();
  }

  @AfterClass
  public static void finish() {
    streamCoordinator.stopAndWait();
    dfsCluster.shutdown();
  }

  @Override
  protected LocationFactory getLocationFactory() {
    return locationFactory;
  }

  @Override
  protected StreamAdmin getStreamAdmin() {
    return streamAdmin;
  }

  @Override
  protected CConfiguration getCConfiguration() {
    return cConf;
  }

  @Override
  protected FileWriter<StreamEvent> createWriter(String streamName) throws IOException {
    StreamConfig config = streamAdmin.getConfig(streamName);
    return fileWriterFactory.create(config, StreamUtils.getGeneration(config));
  }
}
