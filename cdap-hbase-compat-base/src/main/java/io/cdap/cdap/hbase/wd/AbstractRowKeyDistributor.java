/*
 * Copyright © 2014-2017 Cask Data, Inc.
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
package co.cask.cdap.hbase.wd;

import co.cask.cdap.common.utils.ImmutablePair;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
      intervals[i] = new Pair<>(startKeys[i], stopKeys[i]);
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

  /**
   * Method to salt the row key of filter pair. It also adds a new byte with value 1 which means that this byte
   * in provided row key is NOT fixed.
   *
   * @param pair pair with key and value
   * @return list of pairs which has salted row keys
   */
  public List<Pair<byte[], byte[]>> getDistributedFilterPairs(ImmutablePair<byte[], byte[]> pair) {
    List<Pair<byte[], byte[]>> fuzzyPairs = new ArrayList<>();
    byte[][] firstAllDistKeys = getAllDistributedKeys(pair.getFirst());
    for (byte[] firstAllDistKey : firstAllDistKeys) {
      // Only salt the row keys, we do not need to salt mask because we have provided first byte as 1
      // which means that this byte in provided row key is NOT fixed
      fuzzyPairs.add(Pair.newPair(firstAllDistKey, co.cask.cdap.api.common.Bytes.add(new byte[]{1}, pair.getSecond())));
    }
    return fuzzyPairs;
  }

  /**
   * Get all the split keys based on splits and number of buckets.
   *
   * @param splits Number of splits for the table
   * @param buckets Number of buckets for row key distributor
   * @return split keys
   */
  public byte[][] getSplitKeys(int splits, int buckets) {
    // "1" can be used for queue tables that we know are not "hot", so we do not pre-split in this case
    if (splits == 1) {
      return new byte[0][];
    }

    byte[][] bucketSplits = getAllDistributedKeys(co.cask.cdap.api.common.Bytes.EMPTY_BYTE_ARRAY);
    Preconditions.checkArgument(splits >= 1 && splits <= 0xff * bucketSplits.length,
                                "Number of pre-splits should be in [1.." +
                                  0xff * bucketSplits.length + "] range");


    // Splits have format: <salt bucket byte><extra byte>. We use extra byte to allow more splits than buckets:
    // salt bucket bytes are usually sequential in which case we cannot insert any value in between them.

    int splitsPerBucket = (splits + buckets - 1) / buckets;
    splitsPerBucket = splitsPerBucket == 0 ? 1 : splitsPerBucket;

    byte[][] splitKeys = new byte[bucketSplits.length * splitsPerBucket - 1][];

    int prefixesPerSplitInBucket = (0xff + 1) / splitsPerBucket;

    for (int i = 0; i < bucketSplits.length; i++) {
      for (int k = 0; k < splitsPerBucket; k++) {
        if (i == 0 && k == 0) {
          // hbase will figure out first split
          continue;
        }
        int splitStartPrefix = k * prefixesPerSplitInBucket;
        int thisSplit = i * splitsPerBucket + k - 1;
        if (splitsPerBucket > 1) {
          splitKeys[thisSplit] = new byte[] {(byte) i, (byte) splitStartPrefix};
        } else {
          splitKeys[thisSplit] = new byte[] {(byte) i};
        }
      }
    }

    return splitKeys;
  }

}
