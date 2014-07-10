package com.continuuity.data.stream;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.ZKClientModule;
import com.continuuity.data.file.FileWriter;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.runtime.DataSetsModules;
import com.continuuity.data2.transaction.runtime.TransactionMetricsModule;
import com.continuuity.data2.transaction.stream.StreamAdmin;
import com.continuuity.data2.transaction.stream.StreamConfig;
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
      Modules.override(new DataFabricModules().getDistributedModules()).with(new AbstractModule() {

        @Override
        protected void configure() {
          // Tests are running in same process, hence no need to have ZK to coordinate
          bind(StreamCoordinator.class).to(InMemoryStreamCoordinator.class).in(Scopes.SINGLETON);
          bind(StreamAdmin.class).to(TestStreamFileAdmin.class).in(Scopes.SINGLETON);
        }
      }),
      new DataSetsModules().getDistributedModule()
    );

    locationFactory = injector.getInstance(LocationFactory.class);
    streamAdmin = injector.getInstance(StreamAdmin.class);
    fileWriterFactory = injector.getInstance(StreamFileWriterFactory.class);
  }

  @AfterClass
  public static void finish() {
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
