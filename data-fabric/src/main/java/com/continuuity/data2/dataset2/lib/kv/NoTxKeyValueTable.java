package com.continuuity.data2.dataset2.lib.kv;

import com.continuuity.api.dataset.Dataset;

import javax.annotation.Nullable;

/**
 * Non-transactional key-value table
 */
public interface NoTxKeyValueTable extends Dataset {
  void put(byte[] key, @Nullable byte[] value);

  @Nullable
  byte[] get(byte[] key);
}
