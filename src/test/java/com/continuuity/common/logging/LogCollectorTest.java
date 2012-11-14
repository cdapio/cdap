package com.continuuity.common.logging;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Random;

public class LogCollectorTest {

  String makeMessage(int i) {
    return "This is message " + i + ".";
  }

  static String makeTempDir() {
    Random rand = new Random(System.currentTimeMillis());
    while (true) {
      int num = rand.nextInt(Integer.MAX_VALUE);
      String dirName = "tmp_coll" + num;
      File dir = new File(dirName);
      if (dir.mkdir())
        return dirName;
    }
  }

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private void testCollection(CConfiguration config) throws IOException {

    LogCollector collector = new LogCollector(config);
    String logtag = "a:b:c";
    for (int i = 0; i < 30; i++) {
      collector.log(
        new LogEvent(logtag, "ERROR", "this is error message #" + i));
    }
    // collector.close();

    List<String> lines = collector.tail(logtag, 45);
    for (String line : lines) {
      System.out.println(line);
    }
    Assert.assertEquals(1, lines.size());
  }

  @Test
  public void testCollectionLocalFS() throws IOException {

    // create a new temp dir for the log root
    File prefix = tempFolder.newFolder();

    CConfiguration config = CConfiguration.create();
    config.set(Constants.CFG_LOG_COLLECTION_ROOT, prefix.getAbsolutePath());
    config.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");

    testCollection(config);
  }

  @Test
  public void testCollectionDFS() throws IOException {

    MiniDFSCluster dfsCluster = null;

    try {
      File dfsPath = tempFolder.newFolder();

      System.err.println("Starting up Mini HDFS cluster...");
      CConfiguration conf = CConfiguration.create();
      conf.setInt("dfs.block.size", 1024*1024);
      dfsCluster = new MiniDFSCluster.Builder(conf)
          .nameNodePort(0)
          .numDataNodes(1)
          .format(true)
          .manageDataDfsDirs(true)
          .manageNameDfsDirs(true)
          .build();
      dfsCluster.waitClusterUp();
      System.err.println("Mini HDFS is started.");

      // Add HDFS info to conf

      conf.set("fs.defaultFS", dfsCluster.getFileSystem().getUri().toString());
      // set a root directory for log collection
      conf.set(Constants.CFG_LOG_COLLECTION_ROOT, "/logtemp");

      testCollection(conf);
    } finally {
      System.err.println("Shutting down Mini HDFS cluster...");
      dfsCluster.shutdown();
      System.err.println("Mini HDFS is shut down.");
    }
  }

}