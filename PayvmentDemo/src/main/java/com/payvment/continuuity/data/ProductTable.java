package com.payvment.continuuity.data;

import com.continuuity.api.data.BatchCollectionRegistry;
import com.continuuity.api.data.DataFabric;
import com.payvment.continuuity.entity.ProductFeedEntry;
import com.payvment.continuuity.lib.ObjectTable;

public class ProductTable extends ObjectTable<ProductFeedEntry> {
  public ProductTable(DataFabric fabric, BatchCollectionRegistry registry) {
    super(fabric, registry, "ProductTable", ProductFeedEntry.class);
  }

}
