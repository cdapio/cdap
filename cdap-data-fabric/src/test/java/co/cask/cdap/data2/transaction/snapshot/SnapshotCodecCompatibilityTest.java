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
import co.cask.tephra.ChangeId;
import co.cask.tephra.TransactionManager;
import co.cask.tephra.TxConstants;
import co.cask.tephra.persist.TransactionSnapshot;
import co.cask.tephra.snapshot.SnapshotCodecProvider;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
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
}
