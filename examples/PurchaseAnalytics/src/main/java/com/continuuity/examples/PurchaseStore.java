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
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.dataset.table.Table;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.FlowletContext;
import com.google.common.base.Charsets;
import com.google.common.collect.Iterables;
import com.google.common.io.ByteStreams;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.ServiceDiscovered;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Store the incoming items and corresponding user interests, to get analytics on items purchased vs users interests.
 */
public class PurchaseStore extends AbstractFlowlet {

  private static final Logger LOG = LoggerFactory.getLogger(PurchaseStore.class);
  private ServiceDiscovered serviceDiscovered;
  private static final String PREFERENCE_NA = "NotAvailable";

  @UseDataSet("purchasePreference")
  private Table store;

  @Override
  public void initialize(FlowletContext context) {
    //Discover the UserInterestsLookup service via discovery service
    serviceDiscovered = context.discover("PurchaseAnalyticsApp", "UserPreferenceService",
                                         "UserInterestsLookup");
  }

  @ProcessInput
  public void process(Purchase purchase) {
    String userPreference = PREFERENCE_NA;

    Discoverable discoverable = Iterables.getFirst(serviceDiscovered, null);
    if (discoverable != null) {
      // Look up user preference by calling the HTTP service that is started by UserPreferenceService.
      String hostName = discoverable.getSocketAddress().getHostName();
      int port = discoverable.getSocketAddress().getPort();
      userPreference = getUserPreference(hostName, port, purchase.getCustomer());
    }
    // Store the mapping between item purchased and user interest.
    store.put(Bytes.toBytes(purchase.getProduct()), Bytes.toBytes(userPreference), new byte[0]);
  }

  private String getUserPreference(String host, int port, String userId) {
    try {
      URL url = new URL(String.format("http://%s:%d/v1/users/%s/interest", host, port, userId));
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      if (HttpURLConnection.HTTP_OK == conn.getResponseCode()) {
        try {
          return new String(ByteStreams.toByteArray(conn.getInputStream()), Charsets.UTF_8);
        } finally {
          conn.disconnect();
        }
      }
      return PREFERENCE_NA;
    } catch (Throwable th) {
      return PREFERENCE_NA;
    }
  }
}
