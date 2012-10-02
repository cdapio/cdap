package com.continuuity.payvment.data;

import com.continuuity.api.flow.flowlet.FlowletContext;
import com.continuuity.payvment.ProductMeta;

public class ProductTable extends ObjectTable<ProductMeta> {

  public ProductTable(FlowletContext context) {
    super(context, "ProductTable", ProductMeta.class);
  }

}
