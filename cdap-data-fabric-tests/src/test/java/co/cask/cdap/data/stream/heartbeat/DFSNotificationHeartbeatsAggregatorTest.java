/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data.stream.heartbeat;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.TransactionMetricsModule;
import co.cask.cdap.data.stream.InMemoryStreamCoordinator;
import co.cask.cdap.data.stream.StreamCoordinator;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import com.google.inject.AbstractModule;
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

/**
 *
 */
public class DFSNotificationHeartbeatsAggregatorTest extends NotificationHeartbeatsAggregatorTestBase {

  private static StreamAdmin streamAdmin;
  private static MiniDFSCluster dfsCluster;

  @BeforeClass
  public static void init() throws Exception {
    Configuration hConf = new Configuration();
    hConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, tmpFolder.newFolder().getAbsolutePath());
    dfsCluster = new MiniDFSCluster.Builder(hConf).numDataNodes(1).build();
    final FileSystem fileSystem = dfsCluster.getFileSystem();

    CConfiguration cConf = CConfiguration.create();

    Injector injector = createInjector(
      cConf, hConf,
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(LocationFactory.class).toInstance(new HDFSLocationFactory(fileSystem));
        }
      },
      new TransactionMetricsModule(),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      Modules.override(new DataFabricModules().getDistributedModules()).with(new AbstractModule() {

        @Override
        protected void configure() {
          // Tests are running in same process, hence no need to have ZK to coordinate
          bind(StreamCoordinator.class).to(InMemoryStreamCoordinator.class).in(Scopes.SINGLETON);
        }
      }));

    startServices(injector);
    streamAdmin = injector.getInstance(StreamAdmin.class);
  }

  @AfterClass
  public static void finish() throws Exception {
    stopServices();
    dfsCluster.shutdown();
  }

  @Override
  protected StreamAdmin getStreamAdmin() {
    return streamAdmin;
  }
}
