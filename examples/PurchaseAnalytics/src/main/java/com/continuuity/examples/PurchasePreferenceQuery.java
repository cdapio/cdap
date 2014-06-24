package com.continuuity.examples;

/**
 *
 */

import com.continuuity.api.annotation.Handle;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.dataset.table.Row;
import com.continuuity.api.dataset.table.Table;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureResponse;
import com.google.gson.JsonArray;
import com.google.gson.JsonPrimitive;

import java.util.Map;

/**
 * A Procedure for querying the preferences.
 */
public class PurchasePreferenceQuery extends AbstractProcedure {


  @UseDataSet("purchasePreference")
  private Table store;

  @Handle("history")
  @SuppressWarnings("unused")
  public void history(ProcedureRequest request, ProcedureResponder responder) throws Exception {
    String item = request.getArgument("item");
    if (item == null || item.isEmpty()) {
      responder.error(ProcedureResponse.Code.CLIENT_ERROR, "Must pass in item purchased");
    } else {
      Row row = store.get(Bytes.toBytes(item));
      if (row == null) {
        responder.error(ProcedureResponse.Code.NOT_FOUND, "Item not found");
      } else {
         JsonArray array = new JsonArray();
         for (Map.Entry<byte[], byte[]> entry : row.getColumns().entrySet()) {
           array.add(new JsonPrimitive(Bytes.toString(entry.getKey())));
         }
         responder.sendJson(ProcedureResponse.Code.SUCCESS, array);
      }
    }
  }
}
