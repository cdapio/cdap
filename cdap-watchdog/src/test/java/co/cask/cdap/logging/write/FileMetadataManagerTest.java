/*
 * Copyright © 2017 Cask Data, Inc.
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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.kerberos.DefaultOwnerAdmin;
import co.cask.cdap.common.kerberos.OwnerAdmin;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.logging.NamespaceLoggingContext;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.common.namespace.NamespacedLocationFactoryTestClient;
import co.cask.cdap.common.namespace.guice.NamespaceClientRuntimeModule;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.data.runtime.TransactionExecutorModule;
import co.cask.cdap.logging.LoggingConfiguration;
import co.cask.cdap.logging.context.FlowletLoggingContext;
import co.cask.cdap.logging.context.WorkerLoggingContext;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.AuthorizationTestModule;
import co.cask.cdap.security.impersonation.UGIProvider;
import co.cask.cdap.security.impersonation.UnsupportedUGIProvider;
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
 * Tests for FileMetadataManager class.
 */
public class FileMetadataManagerTest {
  private static final Logger LOG = LoggerFactory.getLogger(FileMetadataManagerTest.class);

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final int RETENTION_DURATION_MS = 100000;

  private static Injector injector;
  private static TransactionManager txManager;
  private static String logBaseDir;
  private static NamespaceAdmin namespaceQueryAdmin;
  private static CConfiguration cConf;

  @BeforeClass
  public static void init() throws Exception {
    Configuration hConf = HBaseConfiguration.create();
    cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.set(Constants.CFG_HDFS_NAMESPACE, cConf.get(Constants.CFG_LOCAL_DATA_DIR));
    cConf.set(LoggingConfiguration.LOG_CLEANUP_MAX_NUM_FILES, "10");
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
          bind(OwnerAdmin.class).to(DefaultOwnerAdmin.class);
          bind(UGIProvider.class).to(UnsupportedUGIProvider.class);
        }
      }
    );

    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();
    namespaceQueryAdmin = injector.getInstance(NamespaceAdmin.class);
  }

  @AfterClass
  public static void finish() {
    txManager.stopAndWait();
  }

  @Test
  public void testCleanMetadata() throws Exception {
    FileMetaDataManager fileMetaDataManager = injector.getInstance(FileMetaDataManager.class);
    NamespacedLocationFactory namespacedLocationFactory = injector.getInstance(NamespacedLocationFactory.class);

    // Setup directories for ns
    Location nsContextDir = createContextDir("ns", namespacedLocationFactory);
    LoggingContext flowletContext = new FlowletLoggingContext("ns", "app", "flw", "flwt", "run", "instance");
    Location nsFlowletContextDir = nsContextDir.append("app").append("flw");
    LoggingContext workerContext = new WorkerLoggingContext("ns", "app1", "worker1", "run1", "instance1");
    Location nsWorkerContextDir = nsContextDir.append("app1").append("worker1");
    LoggingContext workerContext2 = new WorkerLoggingContext("ns", "app2", "worker2", "run2", "instance2");
    Location nsWorkerContextDir2 = nsContextDir.append("app2").append("worker2");

    // Setup directories for ns1
    Location nsContextDir1 = createContextDir("ns1", namespacedLocationFactory);
    LoggingContext flowletContext1 = new FlowletLoggingContext("ns1", "app", "flw", "flwt", "run", "instance");
    Location ns1FlowletContextDir = nsContextDir1.append("app").append("flw");
    LoggingContext workerContext1 = new WorkerLoggingContext("ns1", "app1", "worker1", "run1", "instance1");
    Location ns1WorkerContextDir = nsContextDir1.append("app1").append("worker1");

    List<String> collectedMetaFiles = Lists.newArrayList();

    // generate files for ns
    generateFiles(fileMetaDataManager, flowletContext, nsFlowletContextDir, collectedMetaFiles);
    generateFiles(fileMetaDataManager, workerContext, nsWorkerContextDir, collectedMetaFiles);
    generateFiles(fileMetaDataManager, workerContext2, nsWorkerContextDir2, collectedMetaFiles);

    // generate files for ns1
    generateFiles(fileMetaDataManager, flowletContext1, ns1FlowletContextDir, collectedMetaFiles);
    generateFiles(fileMetaDataManager, workerContext1, ns1WorkerContextDir, collectedMetaFiles);

    Assert.assertEquals("Expected number of files are not generated", 100, collectedMetaFiles.size());

    verifyCollectedFiles(fileMetaDataManager, collectedMetaFiles, "ns", 40);
    verifyCollectedFiles(fileMetaDataManager, collectedMetaFiles, "ns1", 0);
    // scan all the files from metadata table
    verifyCollectedFiles(fileMetaDataManager, collectedMetaFiles, null, 0);

  }

  private void verifyCollectedFiles(FileMetaDataManager fileMetaDataManager, List<String> collectedMetaFiles,
                                    final String namespace, int expectedFiles) throws Exception {
    int count = 0;
    FileMetaDataManager.TableKey tableKey;
    tableKey = namespace == null ? null : getTableKey(namespace);

    do {
      FileMetaDataManager.MetaEntryProcessor<Set<URI>> metaFileCollector =
        new FileMetaDataManager.MetaEntryProcessor<Set<URI>>() {
          Set<URI> scannedFiles = new HashSet<>();

          @Override
          public void process(FileMetaDataManager.ScannedEntryInfo scannedFileInfo) {
            scannedFiles.add(scannedFileInfo.getUri());
          }

          @Override
          public Set<URI> getCollectedEntries() {
            return scannedFiles;
          }
        };

      tableKey = fileMetaDataManager.scanFiles(tableKey, 3, metaFileCollector);

      // create location from scanned uris and remove all the metadata files that has corresponding disk file available
      for (final URI uri : metaFileCollector.getCollectedEntries()) {
        count++;
        collectedMetaFiles.remove(uri.getPath());
        LOG.info("count: {}, Removing file {} from deleted list of metaFiles", count, uri);
      }
    } while (tableKey != null);

    Assert.assertEquals("All expected meta files are not scanned", expectedFiles, collectedMetaFiles.size());

  }

  private FileMetaDataManager.TableKey getTableKey(String namespace) {
    LoggingContext loggingContext = new NamespaceLoggingContext(namespace + ":") {
    };
    byte[] logParition = loggingContext.getLogPartition().getBytes();
    return new FileMetaDataManager.TableKey(logParition, Bytes.stopKeyForPrefix(logParition), null);
  }

  private Location createContextDir(String namespace, NamespacedLocationFactory namespacedLocationFactory)
    throws Exception {
    namespaceQueryAdmin.create(new NamespaceMeta.Builder().setName(namespace).build());
    return namespacedLocationFactory.get(Id.Namespace.from(namespace)).append(logBaseDir);
  }

  private void generateFiles(FileMetaDataManager fileMetaDataManager, LoggingContext dummyContext,
                             Location contextDir, List<String> files) throws Exception {
    for (int i = 0; i < 5; i++) {
      generateFile(fileMetaDataManager, dummyContext, contextDir, files, i, 1);
      generateFile(fileMetaDataManager, dummyContext, contextDir, files, i, 2);
      generateFile(fileMetaDataManager, dummyContext, contextDir, files, i, 3);
      generateFile(fileMetaDataManager, dummyContext, contextDir, files, i, 4);
    }
  }

  private void generateFile(FileMetaDataManager fileMetaDataManager, LoggingContext dummyContext,
                            Location contextDir, List<String> files, int i, int fileNumber) throws Exception {
    files.add(contextDir.append("2012-12-1" + i + "/del-" + fileNumber).toURI().getPath());
    Location location = contextDir.append("2012-12-1" + i + "/del-" + fileNumber);
    long deletionBoundary = System.currentTimeMillis() - RETENTION_DURATION_MS;
    long modTime = deletionBoundary - fileNumber - 10000;
    Location file = createFile(location, modTime);
    fileMetaDataManager.writeMetaData(dummyContext, modTime, file);
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
