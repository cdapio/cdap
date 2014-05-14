/**
 * Copyright 2010 Sematext International
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.continuuity.hbase.wd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.Arrays;

/**
 * Defines the way row keys are distributed.
 */
public abstract class AbstractRowKeyDistributor implements Parametrizable {
  public abstract byte[] getDistributedKey(byte[] originalKey);

  public abstract byte[] getOriginalKey(byte[] adjustedKey);

  public abstract byte[][] getAllDistributedKeys(byte[] originalKey);

  /**
   * Gets all distributed intervals based on the original start & stop keys.
   * Used when scanning all buckets based on start/stop row keys. Should return keys so that all buckets in which
   * records between originalStartKey and originalStopKey were distributed are "covered".
   * @param originalStartKey start key
   * @param originalStopKey stop key
   * @return array[Pair(startKey, stopKey)]
   */
  @SuppressWarnings("unchecked")
  public Pair<byte[], byte[]>[] getDistributedIntervals(byte[] originalStartKey, byte[] originalStopKey) {
    byte[][] startKeys = getAllDistributedKeys(originalStartKey);
    byte[][] stopKeys;
    if (Arrays.equals(originalStopKey, HConstants.EMPTY_END_ROW)) {
      Arrays.sort(startKeys, Bytes.BYTES_RAWCOMPARATOR);
      // stop keys are the start key of the next interval
      stopKeys = getAllDistributedKeys(HConstants.EMPTY_BYTE_ARRAY);
      Arrays.sort(stopKeys, Bytes.BYTES_RAWCOMPARATOR);
      for (int i = 0; i < stopKeys.length - 1; i++) {
        stopKeys[i] = stopKeys[i + 1];
      }
      stopKeys[stopKeys.length - 1] = HConstants.EMPTY_END_ROW;
    } else {
      stopKeys = getAllDistributedKeys(originalStopKey);
      assert stopKeys.length == startKeys.length;
    }

    Pair<byte[], byte[]>[] intervals = new Pair[startKeys.length];
    for (int i = 0; i < startKeys.length; i++) {
      intervals[i] = new Pair<byte[], byte[]>(startKeys[i], stopKeys[i]);
    }

    return intervals;
  }

  public final Scan[] getDistributedScans(Scan original) throws IOException {
    Pair<byte[], byte[]>[] intervals = getDistributedIntervals(original.getStartRow(), original.getStopRow());

    Scan[] scans = new Scan[intervals.length];
    for (int i = 0; i < intervals.length; i++) {
      scans[i] = new Scan(original);
      scans[i].setStartRow(intervals[i].getFirst());
      scans[i].setStopRow(intervals[i].getSecond());
    }
    return scans;
  }

  public void addInfo(Configuration conf) {
    conf.set(WdTableInputFormat.ROW_KEY_DISTRIBUTOR_CLASS, this.getClass().getCanonicalName());
    String paramsToStore = getParamsToStore();
    if (paramsToStore != null) {
      conf.set(WdTableInputFormat.ROW_KEY_DISTRIBUTOR_PARAMS, paramsToStore);
    }
  }
}
