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

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.data.dataset.ObjectStore;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.internal.io.UnsupportedTypeException;

/**
 *
 * This implements a simple purchase history application via a scheduled MapReduce Workflow --
 * see package-info for more details.
 */
public class PurchaseApp implements Application {

  @Override
  public ApplicationSpecification configure() {
    try {
      return ApplicationSpecification.Builder.with()
        .setName("PurchaseHistory")
        .setDescription("Purchase history app")
        .withStreams()
          .add(new Stream("purchaseStream"))
        .withDataSets()
          .add(new ObjectStore<PurchaseHistory>("history", PurchaseHistory.class))
          .add(new ObjectStore<Purchase>("purchases", Purchase.class))
          .add(new KeyValueTable("frequentCustomers"))
        .withFlows()
          .add(new PurchaseFlow())
        .withProcedures()
          .add(new PurchaseQuery())
        .noMapReduce()
        .withWorkflows()
          .add(new PurchaseHistoryWorkflow())
        .build();
    } catch (UnsupportedTypeException e) {
      // this exception is thrown by ObjectStore if its parameter type cannot be (de)serialized (for example, if it is
      // an interface and not a class, then there is no auto-magic way deserialize an object. In this case that
      // cannot happen because PurchaseHistory is an actual class.
      throw new RuntimeException(e);
    }
  }
}
