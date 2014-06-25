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

import com.continuuity.api.app.AbstractApplication;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.data.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sample application that uses services.
 */
public class PurchaseAnalyticsApp extends AbstractApplication {

  private static final Logger LOG = LoggerFactory.getLogger(PurchaseAnalyticsApp.class);

  @Override
  public void configure() {
    setDescription("Application that analyzes user preference and their purchase");
    addStream(new Stream("purchaseEvent"));
    addDataSet(new Table("purchasePreference"));
    addFlow(new PurchaseAnalyticsFlow());
    addService(new UserPreferenceService());
    addProcedure(new PurchasePreferenceQuery());
  }
}

