/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.data2.transaction.snapshot;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.CConfigurationUtil;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.tephra.ChangeId;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TransactionType;
import org.apache.tephra.TxConstants;
import org.apache.tephra.persist.TransactionSnapshot;
import org.apache.tephra.persist.TransactionStateStorage;
import org.apache.tephra.runtime.ConfigModule;
import org.apache.tephra.runtime.DiscoveryModules;
import org.apache.tephra.runtime.TransactionModules;
import org.apache.tephra.snapshot.SnapshotCodecProvider;
import org.apache.tephra.snapshot.SnapshotCodecV3;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;


/**
 * Tests snapshot codecs.
 */
public class SnapshotCodecCompatibilityTest {
  @ClassRule
  public static TemporaryFolder tmpDir = new TemporaryFolder();

  @Test
  public void testV1CodecV2Compat() throws Exception {

    long now = System.currentTimeMillis();

    // NOTE: set visibilityUpperBound to 0 as this is expected default for decoding older version that doesn't store it
    TreeMap<Long, TransactionManager.InProgressTx> inProgress =
      Maps.newTreeMap(ImmutableSortedMap.of(16L, new TransactionManager.InProgressTx(0L, now + 1000),
                                            17L, new TransactionManager.InProgressTx(0L, now + 1000)));

    TransactionSnapshot snapshot =
      new TransactionSnapshot(now,
                              15, 18,
                              Lists.newArrayList(5L, 7L),
                              inProgress,
                              ImmutableMap.<Long, Set<ChangeId>>of(17L, Sets.newHashSet(
                                new ChangeId(Bytes.toBytes("ch1")), new ChangeId(Bytes.toBytes("ch2")))),
                              ImmutableMap.<Long, Set<ChangeId>>of(16L, Sets.newHashSet(
                                new ChangeId(Bytes.toBytes("ch2")), new ChangeId(Bytes.toBytes("ch3")))));

    Configuration configV1 = HBaseConfiguration.create();
    configV1.setStrings(TxConstants.Persist.CFG_TX_SNAPHOT_CODEC_CLASSES,
                        SnapshotCodecV1.class.getName());

    SnapshotCodecProvider codecV1 = new SnapshotCodecProvider(configV1);

    // encoding with codec of v1
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try {
      codecV1.encode(out, snapshot);
    } finally {
      out.close();
    }

    // decoding
    Configuration configV1V2 = HBaseConfiguration.create();
    configV1V2.setStrings(TxConstants.Persist.CFG_TX_SNAPHOT_CODEC_CLASSES,
                          SnapshotCodecV1.class.getName(),
                          SnapshotCodecV2.class.getName());
    SnapshotCodecProvider codecV1V2 = new SnapshotCodecProvider(configV1V2);
    TransactionSnapshot decoded = codecV1V2.decode(new ByteArrayInputStream(out.toByteArray()));

    assertEquals(snapshot, decoded);
  }

  /**
   * In-progress LONG transactions written with DefaultSnapshotCodec will not have the type serialized as part of
   * the data.  Since these transactions also contain a non-negative expiration, we need to ensure we reset the type
   * correctly when the snapshot is loaded.
   */
  @Test
  public void testV2ToTephraV3Compatibility() throws Exception {
    long now = System.currentTimeMillis();
    long nowWritePointer = now * TxConstants.MAX_TX_PER_MS;
    /*
     * Snapshot consisting of transactions at:
     */
    long tInvalid = nowWritePointer - 5;    // t1 - invalid
    long readPtr = nowWritePointer - 4;     // t2 - here and earlier committed
    long tLong = nowWritePointer - 3;       // t3 - in-progress LONG
    long tCommitted = nowWritePointer - 2;  // t4 - committed, changeset (r1, r2)
    long tShort = nowWritePointer - 1;      // t5 - in-progress SHORT, canCommit called, changeset (r3, r4)

    TreeMap<Long, TransactionManager.InProgressTx> inProgress = Maps.newTreeMap(ImmutableSortedMap.of(
        tLong, new TransactionManager.InProgressTx(readPtr,
            TransactionManager.getTxExpirationFromWritePointer(tLong, TxConstants.Manager.DEFAULT_TX_LONG_TIMEOUT),
            TransactionManager.InProgressType.LONG),
        tShort, new TransactionManager.InProgressTx(readPtr, now + 1000, TransactionManager.InProgressType.SHORT)));

    TransactionSnapshot snapshot = new TransactionSnapshot(now, readPtr, nowWritePointer,
        Lists.newArrayList(tInvalid), // invalid
        inProgress,
        ImmutableMap.<Long, Set<ChangeId>>of(
            tShort, Sets.newHashSet(new ChangeId(new byte[]{'r', '3'}), new ChangeId(new byte[]{'r', '4'}))),
        ImmutableMap.<Long, Set<ChangeId>>of(
            tCommitted, Sets.newHashSet(new ChangeId(new byte[]{'r', '1'}), new ChangeId(new byte[]{'r', '2'}))));

    Configuration conf1 = new Configuration();
    conf1.set(TxConstants.Persist.CFG_TX_SNAPHOT_CODEC_CLASSES, SnapshotCodecV2.class.getName());
    SnapshotCodecProvider provider1 = new SnapshotCodecProvider(conf1);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try {
      provider1.encode(out, snapshot);
    } finally {
      out.close();
    }

    TransactionSnapshot snapshot2 = provider1.decode(new ByteArrayInputStream(out.toByteArray()));
    assertEquals(snapshot.getReadPointer(), snapshot2.getReadPointer());
    assertEquals(snapshot.getWritePointer(), snapshot2.getWritePointer());
    assertEquals(snapshot.getInvalid(), snapshot2.getInvalid());
    // in-progress transactions will have missing types
    assertNotEquals(snapshot.getInProgress(), snapshot2.getInProgress());
    assertEquals(snapshot.getCommittingChangeSets(), snapshot2.getCommittingChangeSets());
    assertEquals(snapshot.getCommittedChangeSets(), snapshot2.getCommittedChangeSets());

    // after fixing in-progress, full snapshot should match
    Map<Long, TransactionManager.InProgressTx> fixedInProgress = TransactionManager.txnBackwardsCompatCheck(
        TxConstants.Manager.DEFAULT_TX_LONG_TIMEOUT, 10000L, snapshot2.getInProgress());
    assertEquals(snapshot.getInProgress(), fixedInProgress);
    assertEquals(snapshot, snapshot2);
  }

  /**
   * Test full stack serialization for a TransactionManager migrating from DefaultSnapshotCodec to SnapshotCodecV3.
   */
  @Test
  public void testV2ToTephraV3Migration() throws Exception {
    File testDir = tmpDir.newFolder("testV2ToTephraV3Migration");
    Configuration conf = new Configuration();
    conf.setStrings(TxConstants.Persist.CFG_TX_SNAPHOT_CODEC_CLASSES,
        SnapshotCodecV1.class.getName(), SnapshotCodecV2.class.getName());
    conf.set(TxConstants.Manager.CFG_TX_SNAPSHOT_LOCAL_DIR, testDir.getAbsolutePath());

    Injector injector = Guice.createInjector(new ConfigModule(conf),
        new DiscoveryModules().getSingleNodeModules(), new TransactionModules().getSingleNodeModules());

    TransactionManager txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();

    txManager.startLong();

    // shutdown to force a snapshot
    txManager.stopAndWait();

    TransactionStateStorage txStorage = injector.getInstance(TransactionStateStorage.class);
    txStorage.startAndWait();

    // confirm that the in-progress entry is missing a type
    TransactionSnapshot snapshot = txStorage.getLatestSnapshot();
    assertNotNull(snapshot);
    assertEquals(1, snapshot.getInProgress().size());
    Map.Entry<Long, TransactionManager.InProgressTx> entry =
        snapshot.getInProgress().entrySet().iterator().next();
    assertNull(entry.getValue().getType());


    // start a new Tx manager to test fixup
    Configuration conf2 = new Configuration();
    conf2.setStrings(TxConstants.Persist.CFG_TX_SNAPHOT_CODEC_CLASSES,
                    SnapshotCodecV1.class.getName(), SnapshotCodecV2.class.getName(), SnapshotCodecV3.class.getName());
    // make sure we work with the default CDAP conf for snapshot codecs
    CConfiguration cconf = CConfiguration.create();
    CConfigurationUtil.copyTxProperties(cconf, conf2);
    // override snapshot dir
    conf2.set(TxConstants.Manager.CFG_TX_SNAPSHOT_LOCAL_DIR, testDir.getAbsolutePath());

    Injector injector2 = Guice.createInjector(new ConfigModule(conf2),
        new DiscoveryModules().getSingleNodeModules(), new TransactionModules().getSingleNodeModules());

    TransactionManager txManager2 = injector2.getInstance(TransactionManager.class);
    txManager2.startAndWait();

    // state should be recovered
    TransactionSnapshot snapshot2 = txManager2.getCurrentState();
    assertEquals(1, snapshot2.getInProgress().size());
    Map.Entry<Long, TransactionManager.InProgressTx> inProgressTx =
        snapshot2.getInProgress().entrySet().iterator().next();
    assertEquals(TransactionManager.InProgressType.LONG, inProgressTx.getValue().getType());

    // save a new snapshot
    txManager2.stopAndWait();

    TransactionStateStorage txStorage2 = injector2.getInstance(TransactionStateStorage.class);
    txStorage2.startAndWait();

    TransactionSnapshot snapshot3 = txStorage2.getLatestSnapshot();
    // full snapshot should have deserialized correctly without any fixups
    assertEquals(snapshot2.getInProgress(), snapshot3.getInProgress());
    assertEquals(snapshot2, snapshot3);
  }
}
