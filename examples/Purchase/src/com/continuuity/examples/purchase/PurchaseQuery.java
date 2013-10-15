/*
 * Copyright (c) 2013, Continuuity Inc
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms,
 * with or without modification, are not permitted
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.continuuity.examples.purchase;

import com.continuuity.api.annotation.Handle;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.dataset.ObjectStore;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureResponse;

/**
 * A procedure for querying the history dataset for a customer's purchase history.
 */
public class PurchaseQuery extends AbstractProcedure {

  @UseDataSet("history")
  private ObjectStore<PurchaseHistory> store;

  /**
   * Return the specified customer's purchases as a JSON history object.
   * @param request The request, which must contain the "customer" argument. 
   *        Example: history() method with a request parameter for a customer named Tom: history({"customer":"Tom"});
   */
  @Handle("history")
  @SuppressWarnings("unused")
  public void history(ProcedureRequest request, ProcedureResponder responder) throws Exception {
    String customer = request.getArgument("customer");
    PurchaseHistory history = store.read(customer.getBytes());
    if (history == null) {
      responder.error(ProcedureResponse.Code.NOT_FOUND, "No purchase history found for " + customer);
    } else {
      responder.sendJson(new ProcedureResponse(ProcedureResponse.Code.SUCCESS), history);
    }
  }

}
