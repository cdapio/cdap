package com.continuuity.common.logging;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class SyncTest {

  MiniDFSCluster dfsCluster = null;
  Configuration config = null;

  @Before
  public void startDFS() throws IOException {
    config = new Configuration();
    config.setInt("dfs.block.size", 4*1024);
    System.out.println("Starting up Mini DFS cluster...");
    dfsCluster = new MiniDFSCluster.Builder(config)
        .nameNodePort(0)
        .numDataNodes(1)
        .format(true)
        .manageDataDfsDirs(true)
        .manageNameDfsDirs(true)
        .build();
    dfsCluster.waitClusterUp();
    System.out.println("Mini DFS is started.");
    config.set("fs.defaultFS", dfsCluster.getFileSystem().getUri().toString());

  }

  @After
  public void stopDFS() throws IOException {
    System.out.println("Shutting down Mini DFS cluster...");
    dfsCluster.shutdown();
    System.out.println("Mini DFS is shut down.");
  }

  @Test
  public void testSync() throws IOException {
    Path path = new Path("myfile");
    FileSystem fs = FileSystem.get(config);
    FSDataOutputStream out = fs.create(path);
    int numBytes = 5000;
    for (int i = 0; i < numBytes; i++) {
      out.write((byte)i);
      out.hsync();
    }
    out.close();
    Assert.assertTrue(fs.exists(path));
    Assert.assertEquals(numBytes, fs.getFileStatus(path).getLen());
    FSDataInputStream in = fs.open(path);
    byte[] buffer = new byte[numBytes];
    in.readFully(buffer);
    for (int i = 0; i < numBytes; i++) Assert.assertEquals((byte)i, buffer[i]);
    in.close();
  }

}
