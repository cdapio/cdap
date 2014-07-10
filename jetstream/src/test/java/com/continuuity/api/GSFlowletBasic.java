package com.continuuity.api;

import com.continuuity.jetstream.api.AbstractGSFlowlet;
import com.continuuity.jetstream.api.GSSchema;
import com.continuuity.jetstream.api.PrimitiveType;

/**
 *
 */
public class GSFlowletBasic extends AbstractGSFlowlet {

  @Override
  public void create() {
    setName("summation");
    setDescription("sums up the input value over a timewindow");
    GSSchema schema = GSSchema.Builder.with()
      .increasingField("timestamp", PrimitiveType.ULLONG)
      .field("iStream", PrimitiveType.UINT)
      .build();
    addGDATInput("intInput", schema);
    addGSQL("sumOut", "SELECT timestamp, SUM(iStream) as sum FROM intInput GROUP BY timestamp");
  }
}
