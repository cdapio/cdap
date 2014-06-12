package com.continuuity.data2.dataset2;

import com.continuuity.data2.dataset2.lib.AbstractDataset;
import com.continuuity.internal.data.dataset.DatasetSpecification;
import com.continuuity.internal.data.dataset.module.EmbeddedDataSet;

/**
 *
 */
public class DoubleWrappedKVTable extends AbstractDataset implements KeyValueTable {
  private final SimpleKVTable table;

  public DoubleWrappedKVTable(DatasetSpecification spec,
                              @EmbeddedDataSet("data") SimpleKVTable table) {
    super(spec.getName(), table);
    this.table = table;
  }

  public void put(String key, String value) throws Exception {
    table.put(key, value);
  }

  public String get(String key) throws Exception {
    return table.get(key);
  }
}
