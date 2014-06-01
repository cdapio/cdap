package com.continuuity.common.logging;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class LogCollectorTest {

  String makeMessage(int i) {
    return String.format("This is error message %5d.", i);
  }

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private void testCollection(CConfiguration config, Configuration hConfig) throws IOException {

    String logtag = "a:b:c";
    int lengthOfOne = String.format("%s [%s] %s\n", logtag, "ERROR",
        makeMessage(10000)).getBytes(LogFileWriter.CHARSET_UTF8).length;

    // start a log collector with 3 instances and space for 11 messages each
    // (file rolls after a message is logged, hence the extra 2 bytes make
    // space for an extra message.
    config.setInt(LogConfiguration.CFG_ROLL_INSTANCES, 3);
    config.setInt(LogConfiguration.CFG_ROLL_THRESHOLD, 10 * lengthOfOne + 2);
    LogCollector collector = new LogCollector(config, hConfig);

    // write 5 messages, they should all be in the same file
    for (int i = 0; i < 5; i++) {
      collector.log(new LogEvent(logtag, "ERROR", makeMessage(i)));
    }

    // this should return only one message
    List<String> lines = collector.tail(logtag, lengthOfOne + 10);
    Assert.assertEquals(1, lines.size());

    // this should return 3 messages
    lines = collector.tail(logtag, 3 * lengthOfOne + 10);
    Assert.assertEquals(3, lines.size());

    // this should return only 5 messages
    lines = collector.tail(logtag, 10 * lengthOfOne + 10);
    Assert.assertEquals(5, lines.size());

    // test that we can also read with another instance of the collector
    LogCollector collector2 = new LogCollector(config, hConfig);
    // this should return only one message
    lines = collector2.tail(logtag, lengthOfOne + 10);
    Assert.assertEquals(1, lines.size());


    // write 6 more messages, this should roll the log after the last one
    for (int i = 5; i < 11; i++) {
      collector.log(new LogEvent(logtag, "ERROR", makeMessage(i)));
    }

    // this should return only one message, and that is the last one: 10
    lines = collector.tail(logtag, lengthOfOne + 10);
    Assert.assertEquals(1, lines.size());
    Assert.assertTrue(lines.get(0).contains(makeMessage(10)));

    // write 1 more message, this should start writing to the next file
    collector.log(new LogEvent(logtag, "ERROR", makeMessage(11)));

    // this should return two messages: 10 + 11
    lines = collector.tail(logtag, 2 * lengthOfOne + 10);
    Assert.assertEquals(2, lines.size());
    Assert.assertTrue(lines.get(0).contains(makeMessage(10)));
    Assert.assertTrue(lines.get(1).contains(makeMessage(11)));

    // write 24 more messages, this should now roll and evict some
    for (int i = 12; i < 36; i++) {
      collector.log(new LogEvent(logtag, "ERROR", makeMessage(i)));
    }

    // read across all instances: the current file has 3, the others 11
    // hence reading 16 should read across all 3 files
    lines = collector.tail(logtag, 16 * lengthOfOne + 10);
    Assert.assertEquals(16, lines.size());
    for (int i = 0; i < 16; i++) {
      Assert.assertTrue(lines.get(i).contains(makeMessage(i + 20)));
    }

    // read past the last instance: the current file has 3, the others 11
    // hence reading 27 should exceed the 3 files
    lines = collector.tail(logtag, 27 * lengthOfOne + 10);
    Assert.assertEquals(25, lines.size());
    for (int i = 0; i < 25; i++) {
      Assert.assertTrue(lines.get(i).contains(makeMessage(i + 11)));
    }

    // close the log collector and reopen it
    collector.close();
    collector = new LogCollector(config, hConfig);

    // verify that the state is the same with the new collector
    lines = collector.tail(logtag, 27 * lengthOfOne + 10);
    Assert.assertEquals(25, lines.size());
    for (int i = 0; i < 25; i++) {
      Assert.assertTrue(lines.get(i).contains(makeMessage(i + 11)));
    }

    // write 1 more message, this should append to the current file
    collector.log(new LogEvent(logtag, "ERROR", makeMessage(36)));


    // verify that append works and we see an additional message
    // - if it rolls, then we lose messages
    // - if it fails, then we don't see the new message
    lines = collector.tail(logtag, 27 * lengthOfOne + 10);
    Assert.assertEquals(26, lines.size());
    for (int i = 0; i < 26; i++) {
      Assert.assertTrue(lines.get(i).contains(makeMessage(i + 11)));
    }
  }

  @Test
  public void testCollectionLocalFS() throws IOException {

    // create a new temp dir for the log root
    File prefix = tempFolder.newFolder();

    CConfiguration config = CConfiguration.create();
    config.set(Constants.CFG_LOG_COLLECTION_ROOT, prefix.getAbsolutePath());

    Configuration hConf = new Configuration();
    testCollection(config, hConf);
  }

  @Test
  public void testCollectionDFS() throws IOException {

    MiniDFSCluster dfsCluster = null;

    try {
      File dfsPath = tempFolder.newFolder();
      System.setProperty("test.build.data", dfsPath.toString());
      System.setProperty("test.cache.data", dfsPath.toString());

      System.err.println("Starting up Mini HDFS cluster...");
      Configuration hConf = new Configuration();
      CConfiguration conf = CConfiguration.create();
      //conf.setInt("dfs.block.size", 1024*1024);
      dfsCluster = new MiniDFSCluster.Builder(hConf)
          .nameNodePort(0)
          .numDataNodes(1)
          .format(true)
          .manageDataDfsDirs(true)
          .manageNameDfsDirs(true)
          .build();
      dfsCluster.waitClusterUp();
      System.err.println("Mini HDFS is started.");

      // Add HDFS info to conf
      hConf.set("fs.defaultFS", dfsCluster.getFileSystem().getUri().toString());
      // set a root directory for log collection
      conf.set(Constants.CFG_LOG_COLLECTION_ROOT, "/logtemp");

      testCollection(conf, hConf);
    } finally {
      if (dfsCluster != null) {
        System.err.println("Shutting down Mini HDFS cluster...");
        dfsCluster.shutdown();
        System.err.println("Mini HDFS is shut down.");
      }
    }
  }

}
