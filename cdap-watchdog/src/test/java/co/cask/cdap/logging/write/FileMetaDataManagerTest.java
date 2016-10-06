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

package co.cask.cdap.logging.write;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.io.Processor;
import co.cask.cdap.common.io.RootLocationFactory;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.common.namespace.NamespacedLocationFactoryTestClient;
import co.cask.cdap.common.namespace.guice.NamespaceClientRuntimeModule;
import co.cask.cdap.common.security.UGIProvider;
import co.cask.cdap.common.security.UnsupportedUGIProvider;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.data.runtime.TransactionExecutorModule;
import co.cask.cdap.logging.LoggingConfiguration;
import co.cask.cdap.logging.context.FlowletLoggingContext;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.AuthorizationTestModule;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.runtime.TransactionModules;
import org.apache.twill.filesystem.Location;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class FileMetaDataManagerTest {
  private static final Logger LOG = LoggerFactory.getLogger(FileMetaDataManagerTest.class);


  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final int RETENTION_DURATION_MS = 100000;

  private static Injector injector;
  private static TransactionManager txManager;
  private static String logBaseDir;
  private static RootLocationFactory rootLocationFactory;

  @BeforeClass
  public static void init() throws Exception {
    Configuration hConf = HBaseConfiguration.create();
    final CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.set(Constants.CFG_HDFS_NAMESPACE, cConf.get(Constants.CFG_LOCAL_DATA_DIR));
    logBaseDir = cConf.get(LoggingConfiguration.LOG_BASE_DIR);
    injector = Guice.createInjector(
      new ConfigModule(cConf, hConf),
      // use HDFS modules to simulate error cases (local module does not throw the same exceptions as HDFS)
      Modules.override(new LocationRuntimeModule().getDistributedModules()).with(
        new AbstractModule() {
          @Override
          protected void configure() {
            // Override namespace location factory so that it does not perform lookup for NamespaceMeta like
            // DefaultNamespacedLocationFactory does and hence allow unit
            // tests to use it without creating namespace meta for the namespace.
            // This is similar to what NonCustomLocationUnitTestModule does.
            bind(NamespacedLocationFactory.class).to(NamespacedLocationFactoryTestClient.class);
          }
        }
      ),
      new TransactionModules().getInMemoryModules(),
      new TransactionExecutorModule(),
      new DataSetsModules().getInMemoryModules(),
      new SystemDatasetRuntimeModule().getInMemoryModules(),
      new AuthorizationTestModule(),
      new AuthorizationEnforcementModule().getInMemoryModules(),
      new AuthenticationContextModules().getNoOpModule(),
      new NamespaceClientRuntimeModule().getInMemoryModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(UGIProvider.class).to(UnsupportedUGIProvider.class);
        }
      }
    );

    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();
    rootLocationFactory = injector.getInstance(RootLocationFactory.class);
  }

  @AfterClass
  public static void finish() {
    txManager.stopAndWait();
  }


  @Test
  public void testScanFiles() throws Exception {
    FileMetaDataManager fileMetaDataManager = injector.getInstance(FileMetaDataManager.class);
    NamespacedLocationFactory namespacedLocationFactory = injector.getInstance(NamespacedLocationFactory.class);

    // Deletion boundary
    long deletionBoundary = System.currentTimeMillis() - RETENTION_DURATION_MS;
    LOG.info("deletionBoundary = {}", deletionBoundary);

    // Setup directories
    LoggingContext dummyContext = new FlowletLoggingContext("ns", "app", "flw", "flwt", "run", "instance");

    Location namespacedLogsDir = namespacedLocationFactory.get(Id.Namespace.from("ns")).append(logBaseDir);
    Location contextDir = namespacedLogsDir.append("app").append("flw");

    List<Location> expected = Lists.newArrayList();
    for (int i = 0; i < 5; ++i) {
      expected.add(contextDir.append("2012-12-1" + i + "/del-1"));
      expected.add(contextDir.append("2012-12-1" + i + "/del-2"));
      expected.add(contextDir.append("2012-12-1" + i + "/del-3"));
      expected.add(contextDir.append("2012-12-1" + i + "/del-4"));
      expected.add(contextDir.append("del-1"));
    }

    int counter = 0;
    for (Location location : expected) {
      long modTime = deletionBoundary - counter - 10000;
      fileMetaDataManager.writeMetaData(dummyContext, modTime, location);
      counter++;
    }

    FileMetaDataManager.TableKey tableKey = new FileMetaDataManager.TableKey(dummyContext.getLogPartition(), null);

    int fileCount = 0;
    do {
      Processor<URI, Set<URI>> metaProcessor = new Processor<URI, Set<URI>>() {
        private Set<URI> locations = new HashSet<>();

        @Override
        public boolean process(final URI inputUri) {
          locations.add(inputUri);
          return true;
        }

        @Override
        public Set<URI> getResult() {
          return locations;
        }
      };

      tableKey = fileMetaDataManager.scanFiles(tableKey, 5, metaProcessor);
      for (final URI uri : metaProcessor.getResult()) {
        expected.remove(rootLocationFactory.create(uri));
        fileCount++;
      }
    } while (tableKey != null);

    Assert.assertEquals(25, fileCount);
    Assert.assertEquals(ImmutableList.of(), expected);
  }

  @Test
  public void testCleanMetaData() throws Exception {
    FileMetaDataManager fileMetaDataManager = injector.getInstance(FileMetaDataManager.class);
    NamespacedLocationFactory namespacedLocationFactory = injector.getInstance(NamespacedLocationFactory.class);

    // Deletion boundary
    long deletionBoundary = System.currentTimeMillis() - RETENTION_DURATION_MS;
    LOG.info("deletionBoundary = {}", deletionBoundary);

    // Setup directories
    LoggingContext dummyContext = new FlowletLoggingContext("ns", "app", "flw", "flwt", "run", "instance");
    LoggingContext dummyContext1 = new FlowletLoggingContext("ns", "app2", "flw1", "flwt", "run", "instance");
    LoggingContext dummyContext2 = new FlowletLoggingContext("ns2", "app", "flw", "flwt", "run", "instance");

    Location namespacedLogsDir = namespacedLocationFactory.get(Id.Namespace.from("ns")).append(logBaseDir);
    Location namespacedLogsDir2 = namespacedLocationFactory.get(Id.Namespace.from("ns2")).append(logBaseDir);
    Location contextDir = namespacedLogsDir.append("app").append("flw");
    Location contextDir1 = namespacedLogsDir2.append("app2").append("flw1");

    List<Location> toDelete = Lists.newArrayList();
    List<Location> toDelete1 = Lists.newArrayList();
    for (int i = 0; i < 5; ++i) {
      toDelete.add(contextDir.append("2012-12-1" + i + "/del-1"));
      toDelete.add(contextDir.append("2012-12-1" + i + "/del-2"));
      toDelete1.add(contextDir1.append("2012-12-1" + i + "/del-3"));
      toDelete1.add(contextDir1.append("2012-12-1" + i + "/del-4"));
      toDelete.add(contextDir.append("del-1"));
    }

    int counter = 0;
    for (Location location : toDelete) {
      long modTime = deletionBoundary - counter - 10000;
      fileMetaDataManager.writeMetaData(dummyContext, modTime, createFile(location, modTime));
      counter++;
    }

    List<Location> toDelete2 = Lists.newArrayList();
    for (int i = 0; i < 5; ++i) {
      toDelete2.add(contextDir1.append("2012-12-1" + i + "/del-11"));
      toDelete2.add(contextDir1.append("2012-12-1" + i + "/del-12"));
      toDelete2.add(contextDir1.append("2012-12-1" + i + "/del-13"));
      toDelete2.add(contextDir1.append("2012-12-1" + i + "/del-14"));
      toDelete2.add(contextDir1.append("del-11"));
    }

    counter = 0;
    for (Location location : toDelete1) {
      long modTime = deletionBoundary - counter - 10000;
      fileMetaDataManager.writeMetaData(dummyContext1, modTime, createFile(location, modTime));
      counter++;
    }

    counter = 0;
    for (Location location : toDelete2) {
      long modTime = deletionBoundary - counter - 10000;
      fileMetaDataManager.writeMetaData(dummyContext2, modTime, createFile(location, modTime));
      counter++;
    }

    FileMetaDataManager.TableKey tableKey = null;

    do {
      tableKey = fileMetaDataManager.cleanMetaData(tableKey, 2, deletionBoundary,
                                                   new FileMetaDataManager.DeleteCallback() {
                                                     @Override
                                                     public void handle(NamespaceId namespaceId,
                                                                        final Location location,
                                                                        final String namespacedLogBaseDir) {
                                                       try {
                                                         location.delete();
                                                         LOG.info("Deleting: {}", location.toURI().toString());
                                                       } catch (Exception e) {
                                                         LOG.error("Got exception when deleting path {}", location, e);
                                                         throw Throwables.propagate(e);
                                                       }
                                                     }
                                                   });
    } while (tableKey != null);

    for (Location location : toDelete) {
      Assert.assertFalse("Location " + location + " is not deleted!", location.exists());
    }

//    for (Location location : toDelete1) {
//      Assert.assertFalse("ns1 location " + location + " is not deleted!", location.exists());
//    }
  }

  private Location createFile(Location path, long modTime) throws Exception {
    Location parent = Locations.getParent(path);
    Assert.assertNotNull(parent);
    parent.mkdirs();

    path.createNew();
    Assert.assertTrue(path.exists());
    File file = new File(path.toURI());
    Assert.assertTrue(file.setLastModified(modTime));
    return path;
  }
}
