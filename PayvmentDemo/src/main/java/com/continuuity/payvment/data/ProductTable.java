package com.continuuity.payvment.data;

import com.continuuity.api.flow.flowlet.FlowletContext;
import com.continuuity.payvment.entity.ProductFeedEntry;
import com.continuuity.payvment.lib.ObjectTable;

public class ProductTable extends ObjectTable<ProductFeedEntry> {

  public ProductTable(FlowletContext context) {
    super(context, "ProductTable", ProductFeedEntry.class);
  }

}
