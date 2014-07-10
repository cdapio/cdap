package com.continuuity.api;

import com.continuuity.jetstream.api.AbstractGSFlowlet;
import com.continuuity.jetstream.api.GSSchema;
import com.continuuity.jetstream.api.PrimitiveType;

/**
 * Invalid Flowlet since the Schema has two fields with the same name.
 */
public class InvalidGSFlowlet extends AbstractGSFlowlet {

  @Override
  public void create() {
    setName("invalidFlowlet");
    GSSchema schema = GSSchema.Builder.with()
      .field("abcd", PrimitiveType.UINT)
      .field("abcd", PrimitiveType.LLONG)
      .build();
    addGDATInput("invalidInput", schema);
    addGSQL("sql", "SELECT * FROM invalidInput");
  }
}
