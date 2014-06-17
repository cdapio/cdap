package com.continuuity.data2.transaction.snapshot;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.transaction.TxConstants;
import com.continuuity.data2.transaction.inmemory.ChangeId;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.data2.transaction.persist.TransactionSnapshot;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;

/**
 * Tests snapshot codecs.
 */
public class SnapshotCodecCompatibilityTest {

  @Test
  public void testV1CodecV2Compat() throws Exception {

    long now = System.currentTimeMillis();

    // NOTE: set visibilityUpperBound to 0 as this is expected default for decoding older version that doesn't store it
    TreeMap<Long, InMemoryTransactionManager.InProgressTx> inProgress =
      Maps.newTreeMap(ImmutableSortedMap.of(16L, new InMemoryTransactionManager.InProgressTx(0L, now + 1000),
                                            17L, new InMemoryTransactionManager.InProgressTx(0L, now + 1000)));

    TransactionSnapshot snapshot =
      new TransactionSnapshot(now,
                              15, 18,
                              Lists.newArrayList(5L, 7L),
                              inProgress,
                              ImmutableMap.<Long, Set<ChangeId>>of(17L, Sets.newHashSet(
                                new ChangeId(Bytes.toBytes("ch1")), new ChangeId(Bytes.toBytes("ch2")))),
                              ImmutableMap.<Long, Set<ChangeId>>of(16L, Sets.newHashSet(
                                new ChangeId(Bytes.toBytes("ch2")), new ChangeId(Bytes.toBytes("ch3")))));

    CConfiguration configV1 = CConfiguration.create();
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
    CConfiguration configV1V2 = CConfiguration.create();
    configV1V2.setStrings(TxConstants.Persist.CFG_TX_SNAPHOT_CODEC_CLASSES,
                          SnapshotCodecV1.class.getName(),
                          SnapshotCodecV2.class.getName());
    SnapshotCodecProvider codecV1V2 = new SnapshotCodecProvider(configV1V2);
    TransactionSnapshot decoded = codecV1V2.decode(new ByteArrayInputStream(out.toByteArray()));

    assertEquals(snapshot, decoded);
  }
}
