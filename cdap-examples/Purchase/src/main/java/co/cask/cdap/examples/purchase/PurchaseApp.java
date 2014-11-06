/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.examples.purchase;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.lib.ObjectStores;
import co.cask.cdap.internal.io.UnsupportedTypeException;

/**
 *
 * This implements a simple purchase history application via a scheduled MapReduce Workflow --
 * see package-info for more details.
 */
public class PurchaseApp extends AbstractApplication {

  public static final String SERVICE_NAME = "CatalogLookup";

  @Override
  public void configure() {
    setName("PurchaseHistory");
    setDescription("Purchase history app");
    
    // Ingest data into the Application via Streams
    addStream(new Stream("purchaseStream"));
    
    // Store processed data in Datasets
    createDataset("frequentCustomers", KeyValueTable.class);
    
    // Process events in real-time using Flows
    addFlow(new PurchaseFlow());
    
    // Query the processed data using a Procedure
    addProcedure(new PurchaseProcedure());
    
    // Run a MapReduce job on the acquired data using a Workflow
    addWorkflow(new PurchaseHistoryWorkflow());
    
    // Provide a Service to Application components
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
