package com.continuuity.examples.purchase;

import com.continuuity.api.annotation.Handle;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.dataset.ObjectStore;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureResponse;

import java.util.ArrayList;

public class PurchaseQuery extends AbstractProcedure {

  @UseDataSet("purchases")
  private ObjectStore<ArrayList<Purchase>> store;

  @Handle("history")
  @SuppressWarnings("unused")
  public void history(ProcedureRequest request, ProcedureResponder responder) throws Exception {
    String buyer = request.getArgument("buyer");
    ArrayList<Purchase> history = store.read(buyer.getBytes());
    if (history == null) {
      responder.error(ProcedureResponse.Code.NOT_FOUND, "No purchase history for " + buyer);
    } else {
      responder.sendJson(new ProcedureResponse(ProcedureResponse.Code.SUCCESS), history);
    }
  }

}
