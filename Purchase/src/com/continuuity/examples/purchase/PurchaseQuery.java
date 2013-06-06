package com.continuuity.examples.purchase;

import com.continuuity.api.annotation.Handle;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.dataset.ObjectStore;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureResponse;

import java.util.ArrayList;
/**
 * A procedure that allows to query a customer's purchase history.
 */
public class PurchaseQuery extends AbstractProcedure {

  @UseDataSet("history")
  private ObjectStore<PurchaseHistory> store;

  /**
   * Given a customer name, return his purchases as a Json .
   * @param request the request, must contain an argument named "customer"
   */
  @Handle("history")
  @SuppressWarnings("unused")
  public void history(ProcedureRequest request, ProcedureResponder responder) throws Exception {
    String customer = request.getArgument("customer");
    PurchaseHistory history = store.read(customer.getBytes());
    if (history == null) {
      responder.error(ProcedureResponse.Code.NOT_FOUND, "No purchase history for " + customer);
    } else {
      responder.sendJson(new ProcedureResponse(ProcedureResponse.Code.SUCCESS), history);
    }
  }

}
