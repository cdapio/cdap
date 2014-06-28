/**
 * Copyright 2013-2014 Continuuity, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.continuuity.examples.purchase;

import com.continuuity.api.annotation.Handle;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureResponse;

/**
 * A Procedure for querying the history DataSet for a customer's purchase history.
 */
public class PurchaseProcedure extends AbstractProcedure {

  @UseDataSet("history")
  private PurchaseHistoryStore store;

  /**
   * Return the specified customer's purchases as a JSON history object.
   *
   * @param request The request, which must contain the "customer" argument. 
   *        Example: history method with a request parameter for a customer named Tom: history({"customer":"Tom"}).
   */
  @Handle("history")
  @SuppressWarnings("unused")
  public void history(ProcedureRequest request, ProcedureResponder responder) throws Exception {
    String customer = request.getArgument("customer");
    if (customer == null) {
      responder.error(ProcedureResponse.Code.CLIENT_ERROR, "Customer must be given as argument");
      return;
    }
    PurchaseHistory history = store.read(customer);
    if (history == null) {
      responder.error(ProcedureResponse.Code.NOT_FOUND, "No purchase history found for " + customer);
    } else {
      responder.sendJson(new ProcedureResponse(ProcedureResponse.Code.SUCCESS), history);
    }
  }

}
