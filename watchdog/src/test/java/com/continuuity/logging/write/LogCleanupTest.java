package com.continuuity.logging.write;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.logging.LoggingContext;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.InMemoryDataSetAccessor;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTable;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.data2.transaction.inmemory.InMemoryTxSystemClient;
import com.continuuity.logging.context.FlowletLoggingContext;
import com.continuuity.logging.save.LogSaver;
import com.continuuity.weave.filesystem.LocalLocationFactory;
import com.continuuity.weave.filesystem.Location;
import com.continuuity.weave.filesystem.LocationFactory;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * Test LogCleanup class.
 */
public class LogCleanupTest {
  private static final Random RANDOM = new Random(System.currentTimeMillis());
  private static final Logger LOG = LoggerFactory.getLogger(LogCleanupTest.class);

  @ClassRule
  public static final TemporaryFolder tempFolder = new TemporaryFolder();

  private static final int RETENTION_DURATION_MS = 100000;

  private final LocationFactory locationFactory = new LocalLocationFactory();

  @Test
  public void testCleanup() throws Exception {
    CConfiguration cConf = CConfiguration.create();

    DataSetAccessor dataSetAccessor = new InMemoryDataSetAccessor(cConf);
    OrderedColumnarTable metaTable = LogSaver.getMetaTable(dataSetAccessor);

    InMemoryTransactionManager txManager = new InMemoryTransactionManager();
    txManager.startAndWait();
    TransactionSystemClient txClient = new InMemoryTxSystemClient(txManager);
    FileMetaDataManager fileMetaDataManager = new FileMetaDataManager(metaTable, txClient, locationFactory);

    // Create base dir
    Location baseDir = locationFactory.create(tempFolder.newFolder().toURI());

    // Deletion boundary
    long deletionBoundary = System.currentTimeMillis() - RETENTION_DURATION_MS;
    LOG.info("deletionBoundary = {}", deletionBoundary);

    // Setup directories
    LoggingContext dummyContext = new FlowletLoggingContext("act", "app", "flw", "flwt");

    Location contextDir = baseDir.append("act/app/flw");
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

    for (Location location : toDelete) {
      fileMetaDataManager.writeMetaData(dummyContext, deletionBoundary - RANDOM.nextInt(50000) - 10000,
                                        createFile(location));
    }

    for (Location location : notDelete) {
      fileMetaDataManager.writeMetaData(dummyContext, deletionBoundary + RANDOM.nextInt(50000) + 10000,
                                        createFile(location));
    }

    Assert.assertEquals(toDelete.size() + notDelete.size(), fileMetaDataManager.listFiles(dummyContext).size());

    LogCleanup logCleanup = new LogCleanup(locationFactory, fileMetaDataManager,
                                           baseDir,
                                           RETENTION_DURATION_MS);
    logCleanup.run();
    logCleanup.run();

    for (Location location : toDelete) {
      Assert.assertFalse("Location " + location.toURI() + " is not deleted!", location.exists());
    }

    for (Location location : notDelete) {
      Assert.assertTrue("Location " + location.toURI() + " is deleted!", location.exists());
    }

    for (int i = 0; i < 5; ++i) {
      Location delDir = contextDir.append("2012-12-1" + i);
      Assert.assertFalse("Location " + delDir.toURI() + " is not deleted!", delDir.exists());
    }
  }

  @Test
  public void testDeleteEmptyDir1() throws Exception {
    FileSystem fileSystem = FileSystem.get(new Configuration());

    // Create base dir
    Location baseDir = locationFactory.create(tempFolder.newFolder().toURI());

    // Create dirs with files
    Set<Location> files = Sets.newHashSet();
    Set<Location> nonEmptyDirs = Sets.newHashSet();
    for (int i = 0; i < 5; ++i) {
      files.add(createFile(baseDir.append(String.valueOf(i))));

      Location dir1 = createDir(baseDir.append("abc"));
      files.add(dir1);
      nonEmptyDirs.add(dir1);
      files.add(createFile(baseDir.append("abc/" + i)));
      files.add(createFile(baseDir.append("abc/def/" + i)));

      Location dir2 = createDir(baseDir.append("def"));
      files.add(dir2);
      nonEmptyDirs.add(dir2);
      files.add(createFile(baseDir.append("def/" + i)));
      files.add(createFile(baseDir.append("def/hij/" + i)));
    }

    // Create empty dirs
    Set<Location> emptyDirs = Sets.newHashSet();
    for (int i = 0; i < 5; ++i) {
      emptyDirs.add(createDir(baseDir.append("dir_" + i)));
      emptyDirs.add(createDir(baseDir.append("dir_" + i + "/emptyDir1")));
      emptyDirs.add(createDir(baseDir.append("dir_" + i + "/emptyDir2")));

      emptyDirs.add(createDir(baseDir.append("abc/dir_" + i)));
      emptyDirs.add(createDir(baseDir.append("abc/def/dir_" + i)));

      emptyDirs.add(createDir(baseDir.append("def/dir_" + i)));
      emptyDirs.add(createDir(baseDir.append("def/hij/dir_" + i)));
    }

    LogCleanup logCleanup = new LogCleanup(locationFactory, null, baseDir,
                                           RETENTION_DURATION_MS);
    for (Location location : Sets.newHashSet(Iterables.concat(nonEmptyDirs, emptyDirs))) {
      logCleanup.deleteEmptyDir(location);
    }

    // Assert non-empty dirs (and their files) are still present
    for (Location location : files) {
      Assert.assertTrue("Location " + location.toURI() + " is deleted!", location.exists());
    }

    // Assert empty dirs are deleted
    for (Location location : emptyDirs) {
      Assert.assertFalse("Dir " + location.toURI() + " is still present!", location.exists());
    }

    // Assert base dir exists
    Assert.assertTrue(baseDir.exists());

    fileSystem.close();
  }

  @Test
  public void testDeleteEmptyDir2() throws Exception {
    FileSystem fileSystem = FileSystem.get(new Configuration());

    // Create base dir
    Location baseDir = locationFactory.create(tempFolder.newFolder().toURI());

    LogCleanup logCleanup = new LogCleanup(locationFactory, null, baseDir, RETENTION_DURATION_MS);

    logCleanup.deleteEmptyDir(baseDir);
    // Assert base dir exists
    Assert.assertTrue(baseDir.exists());

    Location rootPath = locationFactory.create("/");
    logCleanup.deleteEmptyDir(rootPath);
    // Assert root still exists
    Assert.assertTrue(rootPath.exists());

    Location tmpPath = locationFactory.create("/tmp");
    logCleanup.deleteEmptyDir(tmpPath);
    // Assert tmp still exists
    Assert.assertTrue(tmpPath.exists());

    fileSystem.close();
  }

  private Location createFile(Location path) throws Exception {
    Location parent = LocationUtils.getParent(locationFactory, path);
    parent.mkdirs();

    path.createNew();
    Assert.assertTrue(path.exists());
    return path;
  }

  private Location createDir(Location path) throws Exception {
    path.mkdirs();
    return path;
  }
}
