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

import com.continuuity.api.app.AbstractApplication;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.dataset.lib.KeyValueTable;
import com.continuuity.api.dataset.lib.ObjectStores;
import com.continuuity.internal.io.UnsupportedTypeException;

/**
 *
 * This implements a simple purchase history application via a scheduled MapReduce Workflow --
 * see package-info for more details.
 */
public class PurchaseApp extends AbstractApplication {

  @Override
  public void configure() {
    setName("PurchaseHistory");
    setDescription("Purchase history app");
    addStream(new Stream("purchaseStream"));
    createDataset("frequentCustomers", KeyValueTable.class);
    addFlow(new PurchaseFlow());
    addProcedure(new PurchaseProcedure());
    addWorkflow(new PurchaseHistoryWorkflow());
    addService(new CatalogLookupService());

    try {
      createDataset("history", PurchaseHistoryStore.class, PurchaseHistoryStore.properties());
      ObjectStores.createObjectStore(getConfigurer(), "purchases", Purchase.class);
    } catch (UnsupportedTypeException e) {
      // This exception is thrown by ObjectStore if its parameter type cannot be 
      // (de)serialized (for example, if it is an interface and not a class, then there is
      // no auto-magic way deserialize an object.) In this case that will not happen
      // because PurchaseHistory and Purchase are actual classes.
      throw new RuntimeException(e);
    }
  }
}
