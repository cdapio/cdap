/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.common.logging;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

public class SyncTest {

  MiniDFSCluster dfsCluster = null;
  Configuration config = null;

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Before
  public void startDFS() throws IOException {

    File dfsPath = tempFolder.newFolder();
    System.setProperty("test.build.data", dfsPath.toString());
    System.setProperty("test.cache.data", dfsPath.toString());
    System.out.println("Starting up Mini DFS cluster...");
    config = new HdfsConfiguration();
    // config.setInt("dfs.block.size", 4 * 1024);
    dfsCluster = new MiniDFSCluster.Builder(config)
    //    .nameNodePort(0)
        .numDataNodes(2)
        .format(true)
    //    .manageDataDfsDirs(true)
    //    .manageNameDfsDirs(true)
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

  @Test @Ignore
  public void testSync() throws IOException {
    FileSystem fs = FileSystem.get(config);
    // create a file and write n bytes, then sync
    Path path = new Path("/myfile");
    FSDataOutputStream out = fs.create(path, false, 4096, (short) 2, 4096L);
    int numBytes = 5000;
    for (int i = 0; i < numBytes; i++) {
      out.write((byte) i);
    }
    out.hflush();
    // verify the file is there
    Assert.assertTrue(fs.exists(path));
    // do not verify the length of the file, hflush() does not update that
    //Assert.assertEquals(numBytes, fs.getFileStatus(path).getLen());
    // read back and verify all bytes
    FSDataInputStream in = fs.open(path);
    byte[] buffer = new byte[numBytes];
    in.readFully(buffer);
    for (int i = 0; i < numBytes; i++) {
      Assert.assertEquals((byte) i, buffer[i]);
    }
    in.close();
    // now close the writer
    out.close();
  }

}
