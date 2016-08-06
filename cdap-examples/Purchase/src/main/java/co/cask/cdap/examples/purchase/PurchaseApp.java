/*
 * Copyright © 2014-2016 Cask Data, Inc.
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
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.ObjectMappedTable;
import co.cask.cdap.api.dataset.lib.ObjectMappedTableProperties;

/**
 * This implements a simple purchase history application via a scheduled MapReduce Workflow --
 * see package-info for more details.
 */
public class PurchaseApp extends AbstractApplication {

  public static final String APP_NAME = "PurchaseFlow";

  @Override
  public void configure() {
    setName(APP_NAME);
    setDescription("Purchase flow application");

    // Ingest data into the Application via a Stream
    addStream(new Stream("purchaseStream"));

    // Process events in realtime using a Flow
    addFlow(new PurchaseFlow());

    try {
      createDataset("purchases", ObjectMappedTable.class, ObjectMappedTableProperties.builder().setType(Purchase.class)
        .setDescription("Store purchases").build());
    } catch (UnsupportedTypeException e) {
      // This exception is thrown by ObjectMappedTable if its parameter type cannot be
      // (de)serialized (for example, if it is an interface and not a class, then there is
      // no auto-magic way deserialize an object.) In this case that will not happen
      // because PurchaseHistory and Purchase are actual classes.
      throw new RuntimeException(e);
    }
  }
}
