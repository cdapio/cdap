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

package co.cask.cdap.logging.write;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.io.RootLocationFactory;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.common.namespace.NamespacedLocationFactoryTestClient;
import co.cask.cdap.common.namespace.guice.NamespaceClientRuntimeModule;
import co.cask.cdap.common.security.Impersonator;
import co.cask.cdap.common.security.UGIProvider;
import co.cask.cdap.common.security.UnsupportedUGIProvider;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.data.runtime.TransactionExecutorModule;
import co.cask.cdap.logging.LoggingConfiguration;
import co.cask.cdap.logging.context.FlowletLoggingContext;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.AuthorizationTestModule;
import com.google.common.base.Function;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.runtime.TransactionModules;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;

/**
 * Test LogCleanup class.
 */
public class LogCleanupTest {
  private static final Logger LOG = LoggerFactory.getLogger(LogCleanupTest.class);

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final int RETENTION_DURATION_MS = 100000;
  private static final Random RANDOM = new Random(System.currentTimeMillis());

  private static Injector injector;
  private static TransactionManager txManager;
  private static String logBaseDir;
  private static String namespacesDir;
  private static RootLocationFactory rootLocationFactory;
  private static Impersonator impersonator;
  private static NamespaceAdmin namespaceQueryAdmin;

  @BeforeClass
  public static void init() throws Exception {
    Configuration hConf = HBaseConfiguration.create();
    final CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.set(Constants.CFG_HDFS_NAMESPACE, cConf.get(Constants.CFG_LOCAL_DATA_DIR));
    logBaseDir = cConf.get(LoggingConfiguration.LOG_BASE_DIR);
    namespacesDir = cConf.get(Constants.Namespace.NAMESPACES_DIR);
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
    impersonator = injector.getInstance(Impersonator.class);
    namespaceQueryAdmin = injector.getInstance(NamespaceAdmin.class);
  }

  @AfterClass
  public static void finish() {
    txManager.stopAndWait();
  }


  @Test
  public void testCleanup() throws Exception {
    FileMetaDataManager fileMetaDataManager = injector.getInstance(FileMetaDataManager.class);
    NamespacedLocationFactory namespacedLocationFactory = injector.getInstance(NamespacedLocationFactory.class);

    // Deletion boundary
    long deletionBoundary = System.currentTimeMillis() - RETENTION_DURATION_MS;
    LOG.info("deletionBoundary = {}", deletionBoundary);

    // Setup directories
    LoggingContext dummyContext = new FlowletLoggingContext("ns", "app", "flw", "flwt", "run", "instance");

    Location namespacedLogsDir = namespacedLocationFactory.get(new NamespaceId("ns")).append(logBaseDir);
    Location contextDir = namespacedLogsDir.append("app").append("flw");
    List<Location> toDelete = Lists.newArrayList();
    for (int i = 0; i < 5; ++i) {
      toDelete.add(contextDir.append("2012-12-1" + i + "/del-1"));
      toDelete.add(contextDir.append("2012-12-1" + i + "/del-2"));
      toDelete.add(contextDir.append("2012-12-1" + i + "/del-3"));
      toDelete.add(contextDir.append("2012-12-1" + i + "/del-4"));

      toDelete.add(contextDir.append("del-1"));
    }

    Assert.assertFalse(toDelete.isEmpty());

    List<Location> notDelete = Lists.newArrayList();
    for (int i = 0; i < 5; ++i) {
      toDelete.add(contextDir.append("2012-12-2" + i + "/del-5"));
      notDelete.add(contextDir.append("2012-12-2" + i + "/nodel-1"));
      notDelete.add(contextDir.append("2012-12-2" + i + "/nodel-2"));
      notDelete.add(contextDir.append("2012-12-2" + i + "/nodel-3"));
    }

    Assert.assertFalse(notDelete.isEmpty());

    int counter = 0;
    for (Location location : toDelete) {
      long modTime = deletionBoundary - counter - 10000;
      fileMetaDataManager.writeMetaData(dummyContext, modTime,
                                        createFile(location, modTime));
      counter++;
    }

    for (Location location : notDelete) {
      long modTime = deletionBoundary + counter + 10000;
      fileMetaDataManager.writeMetaData(dummyContext, modTime,
                                        createFile(location, modTime));
      counter++;
    }

    Assert.assertEquals(locationListsToString(toDelete, notDelete),
                        toDelete.size() + notDelete.size(), fileMetaDataManager.listFiles(dummyContext).size());

    // Randomly pick one file from toDelete list and delete it before running log cleanup
    // This is to make sure that when a file is not present, but its metadata is present,
    // log cleanup ignores the absence of the file and cleans up the metadata for the deleted file.
    // Since the file deletion and its metadata clean up are not transactional, we can delete a file but
    // fail to clean up its metadata.
    int index = RANDOM.nextInt(toDelete.size());
    toDelete.get(index).delete();

    LogCleanup logCleanup = new LogCleanup(fileMetaDataManager, rootLocationFactory, namespaceQueryAdmin,
                                           namespacedLocationFactory, logBaseDir, RETENTION_DURATION_MS, impersonator);
    logCleanup.run();
    logCleanup.run();

    for (Location location : toDelete) {
      Assert.assertFalse("Location " + location + " is not deleted!", location.exists());
    }

    for (Location location : notDelete) {
      Assert.assertTrue("Location " + location + " is deleted!", location.exists());
    }

    for (int i = 0; i < 5; ++i) {
      Location delDir = contextDir.append("2012-12-1" + i);
      Assert.assertFalse("Location " + delDir + " is not deleted!", delDir.exists());
    }

    // Assert metadata for all deleted files is gone
    NavigableMap<Long, Location> remainingFilesMap = fileMetaDataManager.listFiles(dummyContext);
    Set<Location> metadataForDeletedFiles =
      Sets.intersection(new HashSet<>(remainingFilesMap.values()), new HashSet<>(toDelete)).immutableCopy();
    Assert.assertEquals(ImmutableSet.of(), metadataForDeletedFiles);
  }

  @Test
  public void testDeleteEmptyDir1() throws Exception {
    // Create base dir
    Location baseDir = rootLocationFactory.create(TEMP_FOLDER.newFolder().toURI());
    // Create namespaced logs dirs
    Location namespacedLogsDir1 = baseDir.append(namespacesDir).append("ns1").append(logBaseDir);
    Location namespacedLogsDir2 = baseDir.append(namespacesDir).append("ns2").append(logBaseDir);
    NamespacedLocationFactory namespacedLocationFactory = injector.getInstance(NamespacedLocationFactory.class);

    // Create dirs with files
    Set<Location> files = Sets.newHashSet();
    Set<Location> nonEmptyDirs = Sets.newHashSet();
    for (int i = 0; i < 1; ++i) {
      String name = String.valueOf(i);
      files.add(createFile(namespacedLogsDir1.append(name), System.currentTimeMillis()));

      Location dir1 = createDir(namespacedLogsDir1.append("abc"));
      files.add(dir1);
      nonEmptyDirs.add(dir1);
      files.add(createFile(namespacedLogsDir1.append("abc").append(name), System.currentTimeMillis()));
      files.add(createFile(namespacedLogsDir1.append("abc").append("def").append(name), System.currentTimeMillis()));

      Location dir2 = createDir(namespacedLogsDir2.append("def"));
      files.add(dir2);
      nonEmptyDirs.add(dir2);
      files.add(createFile(namespacedLogsDir2.append("def").append(name), System.currentTimeMillis()));
      files.add(createFile(namespacedLogsDir2.append("def").append("hij").append(name), System.currentTimeMillis()));
    }

    // Create empty dirs
    Set<Location> emptyDirs = Sets.newHashSet();
    for (int i = 0; i < 1; ++i) {
      emptyDirs.add(createDir(namespacedLogsDir1.append("dir_" + i)));
      emptyDirs.add(createDir(namespacedLogsDir1.append("dir_" + i).append("emptyDir1")));
      emptyDirs.add(createDir(namespacedLogsDir1.append("dir_" + i).append("emptyDir2")));

      emptyDirs.add(createDir(namespacedLogsDir1.append("abc").append("dir_" + i)));
      emptyDirs.add(createDir(namespacedLogsDir1.append("abc").append("def").append("dir_" + i)));

      emptyDirs.add(createDir(namespacedLogsDir2.append("def").append("dir_" + i)));
      emptyDirs.add(createDir(namespacedLogsDir2.append("def").append("hij").append("dir_" + i)));
    }

    LogCleanup logCleanup = new LogCleanup(null, rootLocationFactory, namespaceQueryAdmin, namespacedLocationFactory,
                                           logBaseDir, RETENTION_DURATION_MS, impersonator);
    for (Location location : Sets.newHashSet(Iterables.concat(nonEmptyDirs, emptyDirs))) {
      logCleanup.deleteEmptyDir(namespacedLogsDir1.toString(), location);
      logCleanup.deleteEmptyDir(namespacedLogsDir2.toString(), location);
    }

    // Assert non-empty dirs (and their files) are still present
    for (Location location : files) {
      Assert.assertTrue("Location " + location + " is deleted!", location.exists());
    }

    // Assert empty dirs are deleted
    for (Location location : emptyDirs) {
      Assert.assertFalse("Dir " + location + " is still present!", location.exists());
    }

    // Assert base dir and namespaced log dirs exist
    Assert.assertTrue(baseDir.exists());
    Assert.assertTrue(namespacedLogsDir1.exists());
    Assert.assertTrue(namespacedLogsDir2.exists());
  }

  @Test
  public void testDeleteEmptyDir2() throws Exception {
    // Create base dir
    LocationFactory locationFactory = injector.getInstance(LocationFactory.class);
    Location baseDir = locationFactory.create(TEMP_FOLDER.newFolder().toURI());
    NamespacedLocationFactory namespacedLocationFactory = injector.getInstance(NamespacedLocationFactory.class);

    LogCleanup logCleanup = new LogCleanup(null, rootLocationFactory, namespaceQueryAdmin, namespacedLocationFactory,
                                           logBaseDir, RETENTION_DURATION_MS, impersonator);

    logCleanup.deleteEmptyDir(namespacesDir + "/ns/" + logBaseDir, baseDir);
    // Assert base dir exists
    Assert.assertTrue(baseDir.exists());

    baseDir.mkdirs();
    // Assert root exists
    Assert.assertTrue(baseDir.exists());
    logCleanup.deleteEmptyDir(namespacesDir + "/ns/" + logBaseDir, baseDir);
    // Assert root still exists
    Assert.assertTrue(baseDir.exists());

    Location namespaceDir = baseDir.append(namespacesDir).append("ns");
    namespaceDir.mkdirs();
    Assert.assertTrue(namespaceDir.exists());
    logCleanup.deleteEmptyDir("ns/" + logBaseDir, namespaceDir);
    // Assert root still exists
    Assert.assertTrue(namespaceDir.exists());

    Location tmpPath = locationFactory.create("/tmp");
    tmpPath.mkdirs();
    Assert.assertTrue(tmpPath.exists());
    logCleanup.deleteEmptyDir("ns/" + logBaseDir, tmpPath);
    // Assert tmp still exists
    Assert.assertTrue(tmpPath.exists());
  }

  @Test(expected = FileNotFoundException.class)
  public void testRemoteLocationFactory() throws Exception {
    // Test to make sure that we use a location factory that throws exception when the file modified time is asked
    // for a non-existent file. This is because local location does not throw the same exception as remote location
    // in this case.
    LocationFactory locationFactory = injector.getInstance(LocationFactory.class);
    Location baseDir = locationFactory.create("/non-existent-file-" + System.currentTimeMillis());
    baseDir.lastModified();
  }

  @Test
  public void testCleanupRunWithoutMeta() throws Exception {
    FileMetaDataManager fileMetaDataManager = injector.getInstance(FileMetaDataManager.class);
    NamespacedLocationFactory namespacedLocationFactory = injector.getInstance(NamespacedLocationFactory.class);

    // Deletion boundary
    long deletionBoundary = System.currentTimeMillis() - RETENTION_DURATION_MS;
    LOG.info("deletionBoundary = {}", deletionBoundary);

    // Setup directories
    LoggingContext dummyContext = new FlowletLoggingContext("ns", "app", "flw", "flwt", "run", "instance");

    NamespaceMeta finalMetadata = new NamespaceMeta.Builder().setName("ns").build();
    namespaceQueryAdmin.create(finalMetadata);

    Location namespacedLogsDir = namespacedLocationFactory.get(new NamespaceId("ns")).append(logBaseDir);

    Location contextDir = namespacedLogsDir.append("app").append("flw");
    List<Location> withMetaFiles = Lists.newArrayList();
    List<Location> noMetaFiles = Lists.newArrayList();
    generateFiles(fileMetaDataManager, deletionBoundary, dummyContext, contextDir, withMetaFiles, noMetaFiles);

    LogCleanup logCleanup = new LogCleanup(fileMetaDataManager, rootLocationFactory, namespaceQueryAdmin,
                                           namespacedLocationFactory, logBaseDir, RETENTION_DURATION_MS, impersonator);
    logCleanup.run();

    for (Location location : withMetaFiles) {
      Assert.assertFalse("Location " + location + " is not deleted!", location.exists());
    }

    for (Location location : noMetaFiles) {
      Assert.assertFalse("Location " + location + " is not deleted!", location.exists());
    }

    // Assert all the files with/without metadata is deleted
    for (int i = 0; i < 5; ++i) {
      Location delDir = contextDir.append("2012-12-1" + i);
      Assert.assertFalse("Location " + delDir + " is not deleted!", delDir.exists());
    }

    // Assert metadata for all deleted files is deleted
    NavigableMap<Long, Location> remainingFilesMap = fileMetaDataManager.listFiles(dummyContext);
    Set<Location> metadataForDeletedFiles =
      Sets.intersection(new HashSet<>(remainingFilesMap.values()), new HashSet<>(withMetaFiles)).immutableCopy();
    Assert.assertEquals(ImmutableSet.of(), metadataForDeletedFiles);
  }

  @Test
  public void testCleanFilesWithoutMeta() throws Exception {
    FileMetaDataManager fileMetaDataManager = injector.getInstance(FileMetaDataManager.class);
    NamespacedLocationFactory namespacedLocationFactory = injector.getInstance(NamespacedLocationFactory.class);

    // Deletion boundary
    long deletionBoundary = System.currentTimeMillis() - RETENTION_DURATION_MS;
    LOG.info("deletionBoundary = {}", deletionBoundary);

    // Setup directories
    LoggingContext dummyContext = new FlowletLoggingContext("ns", "app", "flw", "flwt", "run", "instance");

    Location nsContextDir = createContextDir("ns", namespacedLocationFactory);

    List<Location> notToDelete = Lists.newArrayList();
    List<Location> toDelete = Lists.newArrayList();

    generateFiles(fileMetaDataManager, deletionBoundary, dummyContext, nsContextDir, notToDelete, toDelete);
    addFilesModAfterRetention(fileMetaDataManager, deletionBoundary, dummyContext, nsContextDir, notToDelete);

    LogCleanup logCleanup = new LogCleanup(fileMetaDataManager, rootLocationFactory, namespaceQueryAdmin,
                                           namespacedLocationFactory, logBaseDir, RETENTION_DURATION_MS, impersonator);
    final SetMultimap<String, Location> parentDirs = HashMultimap.create();
    final Map<String, NamespaceId> namespaceIdMap = new HashMap<>();


    // Here, we have kept disk scan limit as 15. Since we do not collect files modified after till time, we only have
    // 10 log files with meta and 10 log file without meta. So in worst case 10 + 4, 10 + 4, 10 + 2 we will need 3
    // iterations of cleanFilesWithoutMeta()
    for (int i = 0; i < 3; i++) {
      logCleanup.cleanFilesWithoutMeta(deletionBoundary, namespaceIdMap, parentDirs, 15, 5);
    }

    // Assert only files without metadata is deleted.
    for (Location location : toDelete) {
      Assert.assertFalse("Location " + location + " is not deleted!", location.exists());
    }

    // Assert files with metadata is not deleted
    for (Location location : notToDelete) {
      Assert.assertFalse("Location " + location + " is deleted!", !location.exists());
    }
  }

  @Test
  public void testFilterLocationsWithMeta() throws Exception {
    FileMetaDataManager fileMetaDataManager = injector.getInstance(FileMetaDataManager.class);
    NamespacedLocationFactory namespacedLocationFactory = injector.getInstance(NamespacedLocationFactory.class);

    // Deletion boundary
    long deletionBoundary = System.currentTimeMillis() - RETENTION_DURATION_MS;
    LOG.info("deletionBoundary = {}", deletionBoundary);

    // Setup directories
    LoggingContext dummyContext = new FlowletLoggingContext("ns", "app", "flw", "flwt", "run", "instance");
    Location nsContextDir = createContextDir("ns", namespacedLocationFactory);
    Set<Location> toDelete = new HashSet<>();

    for (int i = 0; i < 5; ++i) {
      toDelete.add(nsContextDir.append("2012-12-1" + i + "/del-1"));
      toDelete.add(nsContextDir.append("2012-12-1" + i + "/del-2"));
      toDelete.add(nsContextDir.append("2012-12-1" + i + "/del-3"));
      toDelete.add(nsContextDir.append("2012-12-1" + i + "/del-4"));
    }

    int counter = 0;
    for (Location location : toDelete) {
      long modTime = deletionBoundary - counter - 10000;
      fileMetaDataManager.writeMetaData(dummyContext, modTime,
                                        createFile(location, modTime));
      counter++;
    }

    LogCleanup logCleanup = new LogCleanup(fileMetaDataManager, rootLocationFactory, namespaceQueryAdmin,
                                           namespacedLocationFactory, logBaseDir, RETENTION_DURATION_MS, impersonator);

    logCleanup.filterLocationsWithMeta(new NamespaceId("ns"), toDelete , 5);

    // Assert files with metadata is not deleted
    Assert.assertEquals(0, toDelete.size());
    // delete these meta files
    logCleanup.run();
  }

  private Location createContextDir(String namespace, NamespacedLocationFactory namespacedLocationFactory)
    throws Exception {
    namespaceQueryAdmin.create(new NamespaceMeta.Builder().setName(namespace).build());
    Location nsNamespacedLogsDir = namespacedLocationFactory.get(new NamespaceId(namespace)).append(logBaseDir);
    return nsNamespacedLogsDir.append("app").append("flw");
  }

  private void generateFiles(FileMetaDataManager fileMetaDataManager, long deletionBoundary,
                             LoggingContext dummyContext, Location contextDir, List<Location> withMetaFiles,
                             List<Location> noMetaFiles) throws Exception {
    for (int i = 0; i < 5; ++i) {
      withMetaFiles.add(contextDir.append("2012-12-1" + i + "/del-1"));
      noMetaFiles.add(contextDir.append("2012-12-1" + i + "/del-2"));
      withMetaFiles.add(contextDir.append("2012-12-1" + i + "/del-3"));
      noMetaFiles.add(contextDir.append("2012-12-1" + i + "/del-4"));
    }

    int counter = 0;
    Iterator<Location> withMetaIterator = withMetaFiles.iterator();
    Iterator<Location> noMetaFilesIterator = noMetaFiles.iterator();
    Iterator<Location> combinedIterator = Iterators.concat(noMetaFilesIterator, withMetaIterator);

    while (combinedIterator.hasNext()) {
      Location location = combinedIterator.next();
      long modTime = deletionBoundary - counter - 10000;
      Location file = createFile(location, modTime);
      if (withMetaFiles.contains(location)) {
        fileMetaDataManager.writeMetaData(dummyContext, modTime, file);
      }
      counter++;
    }
  }

  private void addFilesModAfterRetention(FileMetaDataManager fileMetaDataManager, long deletionBoundary,
                                         LoggingContext dummyContext, Location nsContextDir,
                                         List<Location> notToDelete) throws Exception {
    notToDelete.add(nsContextDir.append("2012-12-15/del-5"));
    notToDelete.add(nsContextDir.append("2012-12-15/del-6"));

    // Add files modified after retention period. Those files should not be deleted
    long modTime = deletionBoundary + 11000;
    fileMetaDataManager.writeMetaData(dummyContext, modTime,
                                      createFile(nsContextDir.append("2012-12-15/del-5"), modTime));

    // Do not write metadata for this file. Then also it should not get deleted during cleanup because, there can be a
    // case where log file on disk is still being created but we already have meta file created for it
    modTime = deletionBoundary + 12000;
    createFile(nsContextDir.append("2012-12-15/del-6"), modTime);
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

  private Location createDir(Location path) throws Exception {
    path.mkdirs();
    return path;
  }

  private String locationListsToString(List<Location> list1, List<Location> list2) {
    return ImmutableList.of(Lists.transform(list1, LOCATION_URI_FUNCTION),
                            Lists.transform(list2, LOCATION_URI_FUNCTION)).toString();
  }

  private static final Function<Location, URI> LOCATION_URI_FUNCTION =
    new Function<Location, URI>() {
      @Override
      public URI apply(Location input) {
        return input.toURI();
      }
    };
}
