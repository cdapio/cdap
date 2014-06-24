package com.continuuity.data2.dataset2.lib.kv;

import com.continuuity.api.dataset.DatasetAdmin;
import com.continuuity.api.dataset.DatasetDefinition;

/**
 *
 */
public class InMemoryKVTableTest extends NoTxKeyValueTableTest {

  @Override
  protected DatasetDefinition<? extends NoTxKeyValueTable, ? extends DatasetAdmin> getDefinition() {
    return new InMemoryKVTableDefinition("foo");
  }
}
