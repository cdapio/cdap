package com.continuuity.examples.purchase;

import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.flow.flowlet.StreamEvent;

/**
 *
 */
public class PurchaseStreamReader extends AbstractFlowlet {

  OutputEmitter<Purchase> out;

  public void process(StreamEvent event) {
    String body = new String(event.getBody().array());
    // <name> bought <n> <items> for $<price>
    String[] tokens =  body.split(" ");
    if (tokens.length != 6) {
      return;
    }
    try {
      Purchase purchase = new Purchase(tokens[0], tokens[3], Integer.parseInt(tokens[2]),
                                       Integer.parseInt(tokens[5].substring(1)));
      out.emit(purchase);
    } catch (Exception e) {
      return;
    }
  }
}
