/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tephra.persist;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.tephra.TxConstants;
import org.apache.tephra.metrics.TxMetricsCollector;
import org.apache.tephra.snapshot.SnapshotCodecProvider;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;


/**
 * Tests persistence of transaction snapshots and write-ahead logs to HDFS storage, using the
 * {@link HDFSTransactionStateStorage} and {@link HDFSTransactionLog} implementations.
 */
public class HDFSTransactionStateStorageTest extends AbstractTransactionStateStorageTest {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static MiniDFSCluster dfsCluster;
  private static Configuration conf;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration hConf = new Configuration();
    hConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, tmpFolder.newFolder().getAbsolutePath());

    dfsCluster = new MiniDFSCluster.Builder(hConf).numDataNodes(1).build();
    conf = new Configuration(dfsCluster.getFileSystem().getConf());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    dfsCluster.shutdown();
  }

  @Override
  protected Configuration getConfiguration(String testName) throws IOException {
    // tests should use the current user for HDFS
    conf.unset(TxConstants.Manager.CFG_TX_HDFS_USER);
    conf.set(TxConstants.Manager.CFG_TX_SNAPSHOT_DIR, tmpFolder.newFolder().getAbsolutePath());
    return conf;
  }

  @Override
  protected AbstractTransactionStateStorage getStorage(Configuration conf) {
    return new HDFSTransactionStateStorage(conf, new SnapshotCodecProvider(conf), new TxMetricsCollector());
  }
}
