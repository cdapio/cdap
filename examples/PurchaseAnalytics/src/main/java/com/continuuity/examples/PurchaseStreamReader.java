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

package com.continuuity.examples;

import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.flow.flowlet.StreamEvent;

/**
 * Reader to read stream events from purchaseStream.
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
