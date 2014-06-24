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
    setName("PurchseAnalyticsApp");
    setDescription("Application that analyzes user preference and their purchase");
    addStream(new Stream("purchaseEvent"));
    addDataSet(new Table("purchasePreference"));
    addFlow(new PurchaseAnalyticsFlow());
    addService(new UserPreferenceService());
  }
}

