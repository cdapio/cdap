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

import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;

/**
 * This is a simple Flow that consumes purchase events from a Stream and stores Purchase objects in datastore.
 * It has only two Flowlets: one consumes events from the Stream and converts them into Purchase objects,
 * the other consumes these objects and stores them in a DataSet.
 */
public class PurchaseFlow implements Flow {

  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName("PurchaseFlow")
      .setDescription("Reads user and purchase information and stores in dataset")
      .withFlowlets()
        .add("reader", new PurchaseStreamReader())
        .add("collector", new PurchaseStore())
      .connect()
        .fromStream("purchaseStream").to("reader")
        .from("reader").to("collector")
      .build();
  }
}
