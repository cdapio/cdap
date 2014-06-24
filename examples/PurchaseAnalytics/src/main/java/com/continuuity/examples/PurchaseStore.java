package com.continuuity.examples;

import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.dataset.table.Table;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.FlowletContext;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.ServiceDiscovered;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

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
    Iterator<Discoverable> discoverables = serviceDiscovered.iterator();
    String userPreference = PREFERENCE_NA;
    // Look up user preference by calling the HTTP service that is started by UserPreferenceService.
    if (discoverables.hasNext()) {
      Discoverable discoverable = discoverables.next();
      String hostName = discoverable.getSocketAddress().getHostName();
      int port = discoverable.getSocketAddress().getPort();
      userPreference = getUserPreference(hostName, port, purchase.getCustomer());
    }
    // Store the mapping between item purchased and user interest.
    store.put(Bytes.toBytes(purchase.getProduct()), Bytes.toBytes(userPreference), new byte[0]);
  }

  private String getUserPreference(String host, int port, String userId){
    try {
      DefaultHttpClient client = new DefaultHttpClient();
      HttpGet get = new HttpGet(String.format("http://%s:%d/v1/users/%s/interest", host, port, userId));

      HttpResponse response = client.execute(get);
      if (response.getStatusLine().getStatusCode() == 200) {
        return EntityUtils.toString(response.getEntity());
      }
    } catch (Throwable th) {
      LOG.error("Caught exception in looking up user preference {}", th.getCause(), th);
    }
    return PREFERENCE_NA;
  }

}
