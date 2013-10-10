package com.continuuity.logging.save;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.logging.LoggingContext;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.InMemoryDataSetAccessor;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTable;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.data2.transaction.inmemory.InMemoryTxSystemClient;
import com.continuuity.logging.context.FlowletLoggingContext;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * Test LogCleanup class.
 */
public class LogCleanupTest {
  private static final Random RANDOM = new Random(System.currentTimeMillis());

  @ClassRule
  public static final TemporaryFolder tempFolder = new TemporaryFolder();

  private static final int RETENTION_DURATION_MS = 100000;

  @Test
  public void testCleanup() throws Exception {
    CConfiguration cConf = CConfiguration.create();

    DataSetAccessor dataSetAccessor = new InMemoryDataSetAccessor(cConf);
    OrderedColumnarTable metaTable = LogSaver.getMetaTable(dataSetAccessor);
    FileSystem fileSystem = FileSystem.get(new Configuration());

    InMemoryTransactionManager txManager = new InMemoryTransactionManager();
    txManager.startAndWait();
    TransactionSystemClient txClient = new InMemoryTxSystemClient(txManager);
    FileMetaDataManager fileMetaDataManager = new FileMetaDataManager(metaTable, txClient);

    // Create base dir
    Path baseDir = new Path(tempFolder.newFolder().toURI());

    // Deletion boundary
    long deletionBoundary = System.currentTimeMillis() - RETENTION_DURATION_MS;

    // Setup directories
    LoggingContext dummyContext = new FlowletLoggingContext("act", "app", "flw", "flwt");

    Path contextDir = new Path(baseDir, "act/app/flw");
    List<Path> toDelete = Lists.newArrayList();
    for (int i = 0; i < 5; ++i) {
      toDelete.add(new Path(contextDir, "2012-12-1" + i + "/del-1"));
      toDelete.add(new Path(contextDir, "2012-12-1" + i + "/del-2"));
      toDelete.add(new Path(contextDir, "2012-12-1" + i + "/del-3"));
      toDelete.add(new Path(contextDir, "2012-12-1" + i + "/del-4"));

      toDelete.add(new Path(contextDir, "del-1"));
    }

    Assert.assertFalse(toDelete.isEmpty());

    List<Path> notDelete = Lists.newArrayList();
    for (int i = 0; i < 5; ++i) {
      toDelete.add(new Path(contextDir, "2012-12-2" + i + "/del-5"));
      notDelete.add(new Path(contextDir, "2012-12-2" + i + "/nodel-1"));
      notDelete.add(new Path(contextDir, "2012-12-2" + i + "/nodel-2"));
      notDelete.add(new Path(contextDir, "2012-12-2" + i + "/nodel-3"));
    }

    Assert.assertFalse(notDelete.isEmpty());

    for (Path path : toDelete) {
      fileMetaDataManager.writeMetaData(dummyContext, deletionBoundary - RANDOM.nextInt(50000) - 10000,
                                        createFile(fileSystem, path));
    }

    for (Path path : notDelete) {
      fileMetaDataManager.writeMetaData(dummyContext, deletionBoundary + RANDOM.nextInt(50000) + 10000,
                                        createFile(fileSystem, path));
    }

    Assert.assertEquals(toDelete.size() + notDelete.size(), fileMetaDataManager.listFiles(dummyContext).size());

    LogCleanup logCleanup = new LogCleanup(fileSystem, fileMetaDataManager, baseDir, RETENTION_DURATION_MS);
    logCleanup.run();
    logCleanup.run();

    for (Path path : toDelete) {
      Assert.assertFalse("Path " + path + " is not deleted!", fileSystem.exists(path));
    }

    for (Path path : notDelete) {
      Assert.assertTrue("Path " + path + " is deleted!", fileSystem.exists(path));
    }

    for (int i = 0; i < 5; ++i) {
      Path delDir = new Path(contextDir, "2012-12-1" + i);
      Assert.assertFalse("Path " + delDir + " is not deleted!", fileSystem.exists(delDir));
    }
  }

  @Test
  public void testDeleteEmptyDir1() throws Exception {
    FileSystem fileSystem = FileSystem.get(new Configuration());

    // Create base dir
    Path baseDir = new Path(tempFolder.newFolder().toURI());

    // Create dirs with files
    Set<Path> files = Sets.newHashSet();
    for (int i = 0; i < 5; ++i) {
      files.add(createFile(fileSystem, new Path(baseDir, String.valueOf(i))));

      files.add(createDir(fileSystem, new Path(baseDir, "abc")));
      files.add(createFile(fileSystem, new Path(baseDir, "abc/" + i)));
      files.add(createFile(fileSystem, new Path(baseDir, "abc/def/" + i)));

      files.add(createDir(fileSystem, new Path(baseDir, "def")));
      files.add(createFile(fileSystem, new Path(baseDir, "def/" + i)));
      files.add(createFile(fileSystem, new Path(baseDir, "def/hij/" + i)));
    }

    // Create empty dirs
    Set<Path> emptyDirs = Sets.newHashSet();
    for (int i = 0; i < 5; ++i) {
      emptyDirs.add(createDir(fileSystem, new Path(baseDir, "dir_" + i)));
      emptyDirs.add(createDir(fileSystem, new Path(baseDir, "dir_" + i + "/emptyDir1")));
      emptyDirs.add(createDir(fileSystem, new Path(baseDir, "dir_" + i + "/emptyDir2")));

      emptyDirs.add(createDir(fileSystem, new Path(baseDir, "abc/dir_" + i)));
      emptyDirs.add(createDir(fileSystem, new Path(baseDir, "abc/def/dir_" + i)));

      emptyDirs.add(createDir(fileSystem, new Path(baseDir, "def/dir_" + i)));
      emptyDirs.add(createDir(fileSystem, new Path(baseDir, "def/hij/dir_" + i)));
    }

    LogCleanup logCleanup = new LogCleanup(fileSystem, null, baseDir, RETENTION_DURATION_MS);
    for (Path path : Sets.newHashSet(Iterables.concat(files, emptyDirs))) {
      logCleanup.deleteEmptyDir(path);
    }

    // Assert non-empty dirs (and their files) are still present
    for (Path path : files) {
      Assert.assertTrue("Path " + path + " is deleted!", fileSystem.exists(path));
    }

    // Assert empty dirs are deleted
    for (Path path : emptyDirs) {
      Assert.assertFalse("Dir " + path + " is still present!", fileSystem.exists(path));
    }

    // Assert base dir exists
    Assert.assertTrue(fileSystem.exists(baseDir));

    fileSystem.close();
  }

  @Test
  public void testDeleteEmptyDir2() throws Exception {
    FileSystem fileSystem = FileSystem.get(new Configuration());

    // Create base dir
    Path baseDir = new Path(tempFolder.newFolder().toURI());

    LogCleanup logCleanup = new LogCleanup(fileSystem, null, baseDir, RETENTION_DURATION_MS);

    logCleanup.deleteEmptyDir(baseDir);
    // Assert base dir exists
    Assert.assertTrue(fileSystem.exists(baseDir));

    Path rootPath = new Path("/");
    logCleanup.deleteEmptyDir(rootPath);
    // Assert root still exists
    Assert.assertTrue(fileSystem.exists(rootPath));

    fileSystem.close();
  }

  @Test
  public void testNormalize() throws Exception {
    Assert.assertEquals(new Path("/"), LogCleanup.normalize(new Path("/")));

    Assert.assertEquals(new Path("/abc"), LogCleanup.normalize(new Path("/abc")));
    Assert.assertEquals(new Path("/abc"), LogCleanup.normalize(new Path("/abc/")));
    Assert.assertEquals(new Path("/abc"), LogCleanup.normalize(new Path("/abc//")));

    Assert.assertEquals(new Path("/abc/def"), LogCleanup.normalize(new Path("/abc/def")));
    Assert.assertEquals(new Path("/abc/def"), LogCleanup.normalize(new Path("/abc/def/")));
    Assert.assertEquals(new Path("/abc/def"), LogCleanup.normalize(new Path("/abc/def//")));
    Assert.assertEquals(new Path("/abc/def"), LogCleanup.normalize(new Path("/abc/def///")));
  }

  private static Path createFile(FileSystem fileSystem, Path path) throws Exception {
    if (!fileSystem.exists(path.getParent())) {
      fileSystem.mkdirs(path.getParent());
    }

    FSDataOutputStream fsDataOutputStream = fileSystem.create(path);
    fsDataOutputStream.close();
    return path;
  }

  private static Path createDir(FileSystem fileSystem, Path path) throws Exception {
    fileSystem.mkdirs(path);
    return path;
  }
}
