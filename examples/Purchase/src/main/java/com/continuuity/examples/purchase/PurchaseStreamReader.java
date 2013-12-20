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

import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.flow.flowlet.StreamEvent;

/**
 * This flowlet reads events from a stream and parses them as sentences of the form
 * <pre><name> bought <n> <items> for $<price></pre>. The event is then converted into
 * a Purchase object and emitted. If the event does not have this form, it is dropped.
 */
public class PurchaseStreamReader extends AbstractFlowlet {

  OutputEmitter<Purchase> out;

  @ProcessInput
  public void process(StreamEvent event) {
    String body = new String(event.getBody().array());
    // <name> bought <n> <items> for $<price>
    String[] tokens =  body.split(" ");
    if (tokens.length != 6) {
      return;
    }
    String customer = tokens[0];
    if (!"bought".equals(tokens[1])) {
      return;
    }
    int quantity = Integer.parseInt(tokens[2]);
    String item = tokens[3];
    if (quantity != 1 && item.length() > 1) {
      item = item.substring(0, item.length() - 1);
    }
    if (!"for".equals(tokens[4])) {
      return;
    }
    String price = tokens[5];
    if (!price.startsWith("$")) {
      return;
    }
    int amount = Integer.parseInt(tokens[5].substring(1));

    Purchase purchase = new Purchase(customer, item, quantity, amount, System.currentTimeMillis());
    out.emit(purchase);
  }
}
