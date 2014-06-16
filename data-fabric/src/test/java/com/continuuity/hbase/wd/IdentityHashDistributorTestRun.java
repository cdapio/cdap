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

/**
 *
 */
public class IdentityHashDistributorTestRun extends RowKeyDistributorTestBase {
  public IdentityHashDistributorTestRun() {
    super(new RowKeyDistributorByHashPrefix(new IdentityHash()));
  }

  /**
   *
   */
  public static class IdentityHash implements RowKeyDistributorByHashPrefix.Hasher {
    private static final byte[] EMPTY_PREFIX = new byte[0];

    @Override
    public byte[] getHashPrefix(byte[] originalKey) {
      return EMPTY_PREFIX;
    }

    @Override
    public byte[][] getAllPossiblePrefixes() {
      return new byte[][] {EMPTY_PREFIX};
    }

    @Override
    public int getPrefixLength(byte[] adjustedKey) {
      // the original key wasn't changed
      return 0;
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
