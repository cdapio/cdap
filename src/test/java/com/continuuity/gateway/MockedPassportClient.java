package com.continuuity.gateway;

import java.util.List;
import java.util.Map;

import com.continuuity.passport.http.client.PassportClient;

/**
 * Mocks a PassportClient by returning values based on the intialized data only.
 */
public class MockedPassportClient extends PassportClient {

  private final Map<String,List<String>> keysAndClusters;

  public MockedPassportClient(Map<String,List<String>> keysAndClusters) {
    this.keysAndClusters = keysAndClusters;
  }

  @Override
  public List<String> getVPCList(String hostname, String apiKey)
      throws Exception {
    return this.keysAndClusters.get(apiKey);
  }
}
