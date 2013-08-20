package com.continuuity.logging.save;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;
import java.util.Set;

/**
 * Test LogCleanup class.
 */
public class LogCleanupTest {
  @Test
  public void testDeleteEmptyDir1() throws Exception {
    Random random = new Random(System.currentTimeMillis());
    FileSystem fileSystem = FileSystem.get(new Configuration());

    // Create base dir
    Path baseDir = new Path(String.format("%s/testDeleteEmptyDir_%s_%s",
                                          System.getProperty("java.io.tmpdir"),
                                          System.currentTimeMillis(),
                                          random.nextLong()));
    createTempDir(fileSystem, baseDir);

    // Create dirs with files
    Set<Path> files = Sets.newHashSet();
    for (int i = 0; i < 5; ++i) {
      files.add(createTempFile(fileSystem, new Path(baseDir, String.valueOf(i))));

      files.add(createTempDir(fileSystem, new Path(baseDir, "abc")));
      files.add(createTempFile(fileSystem, new Path(baseDir, "abc/" + i)));
      files.add(createTempFile(fileSystem, new Path(baseDir, "abc/def/" + i)));

      files.add(createTempDir(fileSystem, new Path(baseDir, "def")));
      files.add(createTempFile(fileSystem, new Path(baseDir, "def/" + i)));
      files.add(createTempFile(fileSystem, new Path(baseDir, "def/hij/" + i)));
    }

    // Create empty dirs
    Set<Path> emptyDirs = Sets.newHashSet();
    for (int i = 0; i < 5; ++i) {
      emptyDirs.add(createTempDir(fileSystem, new Path(baseDir, "dir_" + i)));
      emptyDirs.add(createTempDir(fileSystem, new Path(baseDir, "dir_" + i + "/emptyDir1")));
      emptyDirs.add(createTempDir(fileSystem, new Path(baseDir, "dir_" + i + "/emptyDir2")));

      emptyDirs.add(createTempDir(fileSystem, new Path(baseDir, "abc/dir_" + i)));
      emptyDirs.add(createTempDir(fileSystem, new Path(baseDir, "abc/def/dir_" + i)));

      emptyDirs.add(createTempDir(fileSystem, new Path(baseDir, "def/dir_" + i)));
      emptyDirs.add(createTempDir(fileSystem, new Path(baseDir, "def/hij/dir_" + i)));
    }

    LogCleanup logCleanup = new LogCleanup(fileSystem, null, baseDir, 1000000);
    for (Path path : Sets.newHashSet(Iterables.concat(files, emptyDirs))) {
      logCleanup.deleteEmptyDir(path);
    }

    // Assert non-empty dirs (and their files) are still present
    for (Path path : files) {
      Assert.assertTrue(fileSystem.exists(path));
    }

    // Assert empty dirs are deleted
    for (Path path : emptyDirs) {
      Assert.assertFalse(fileSystem.exists(path));
    }

    // Assert base dir exists
    Assert.assertTrue(fileSystem.exists(baseDir));

    fileSystem.close();
  }

  @Test
  public void testDeleteEmptyDir2() throws Exception {
    Random random = new Random(System.currentTimeMillis());
    FileSystem fileSystem = FileSystem.get(new Configuration());

    // Create base dir
    Path baseDir = new Path(String.format("%s/testDeleteEmptyDir_%s_%s",
                                          System.getProperty("java.io.tmpdir"),
                                          System.currentTimeMillis(),
                                          random.nextLong()));
    createTempDir(fileSystem, baseDir);

    LogCleanup logCleanup = new LogCleanup(fileSystem, null, baseDir, 1000000);

    logCleanup.deleteEmptyDir(baseDir);
    // Assert base dir exists
    Assert.assertTrue(fileSystem.exists(baseDir));

    Path rootPath = new Path("/");
    logCleanup.deleteEmptyDir(rootPath);
    // Assert root still exists
    Assert.assertTrue(fileSystem.exists(rootPath));

    fileSystem.close();
  }

  private static Path createTempFile(FileSystem fileSystem, Path path) throws Exception {
    FSDataOutputStream fsDataOutputStream = fileSystem.create(path);
    fsDataOutputStream.close();
    fileSystem.deleteOnExit(path);
    return path;
  }

  private static Path createTempDir(FileSystem fileSystem, Path path) throws Exception {
    fileSystem.mkdirs(path);
    fileSystem.deleteOnExit(path);
    return path;
  }
}
