package com.payvment.continuuity.data;

import com.continuuity.api.data.lib.ObjectTable;
import com.payvment.continuuity.entity.ProductFeedEntry;

public class ProductTable extends ObjectTable<ProductFeedEntry> {
  public ProductTable() {
    super("ProductTable", ProductFeedEntry.class);
  }

}
