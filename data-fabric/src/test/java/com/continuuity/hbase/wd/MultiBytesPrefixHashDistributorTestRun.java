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

import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 */
public class MultiBytesPrefixHashDistributorTestRun extends RowKeyDistributorTestBase {
  public MultiBytesPrefixHashDistributorTestRun() {
    super(new RowKeyDistributorByHashPrefix(new MultiBytesPrefixHash()));
  }

  /**
   *
   */
  public static class MultiBytesPrefixHash implements RowKeyDistributorByHashPrefix.Hasher {
    private static final byte[] PREFIX1 = new byte[] {(byte) 3, (byte) 23};
    private static final byte[] PREFIX2 = new byte[] {(byte) 1, (byte) 55};
    private static final byte[] PREFIX3 = new byte[] {(byte) 2, (byte) 55};
    private static final byte[][] ALL_PREFIXES = new byte[][] {PREFIX1, PREFIX2, PREFIX3};

    @Override
    public byte[] getHashPrefix(byte[] originalKey) {
      return ALL_PREFIXES[Math.abs(Bytes.hashCode(originalKey) % 3)]; // close to random prefix
    }

    @Override
    public byte[][] getAllPossiblePrefixes() {
      return ALL_PREFIXES;
    }

    @Override
    public int getPrefixLength(byte[] adjustedKey) {
      return PREFIX1.length;
    }

    @Override
    public String getParamsToStore() {
      return null;
    }

    @Override
    public void init(String storedParams) {
      // DO NOTHING
    }
  }
}
