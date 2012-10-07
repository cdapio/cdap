package com.payvment.continuuity.data;

import com.continuuity.api.data.BatchCollectionRegistry;
import com.continuuity.api.data.DataFabric;
import com.continuuity.api.data.lib.ObjectTable;
import com.payvment.continuuity.entity.ProductFeedEntry;

public class ProductTable extends ObjectTable<ProductFeedEntry> {
  public ProductTable(DataFabric fabric, BatchCollectionRegistry registry) {
    super(fabric, registry, "ProductTable", ProductFeedEntry.class);
  }

}
