package com.continuuity.gateway;

import com.continuuity.data.Constants;
import com.continuuity.passport.http.client.AccountProvider;
import com.continuuity.passport.http.client.PassportClient;
import com.continuuity.passport.meta.Account;

import java.util.List;
import java.util.Map;

/**
 * Mocks a PassportClient by returning values based on the intialized data only.
 */
public class MockedPassportClient extends PassportClient {

  private final Map<String,List<String>> keysAndClusters;

  public MockedPassportClient(Map<String,List<String>> keysAndClusters) {
    this.keysAndClusters = keysAndClusters;
  }

  @Override
  public List<String> getVPCList(String apiKey)
      throws RuntimeException {
    return this.keysAndClusters.get(apiKey);
  }

  @Override
  public AccountProvider<Account> getAccount(String apiKey)
    throws RuntimeException {
    return new AccountProvider<Account>(new Account("John","Smith", "john@smith.com")) {
      @Override
      public String getAccountId() {
        return Constants.DEVELOPER_ACCOUNT_ID;
      }
    };
  }
}
