/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.gateway;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.passport.http.client.AccountProvider;
import co.cask.cdap.passport.http.client.PassportClient;
import co.cask.cdap.passport.meta.Account;

import java.util.List;
import java.util.Map;

/**
 * Mocks a PassportClient by returning values based on the intialized data only.
 */
public class MockedPassportClient extends PassportClient {

  private final Map<String, List<String>> keysAndClusters;

  public MockedPassportClient(Map<String, List<String>> keysAndClusters) {
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
    return new AccountProvider<Account>(new Account("John", "Smith", "john@smith.com")) {
      @Override
      public String getAccountId() {
        return Constants.DEVELOPER_ACCOUNT_ID;
      }
    };
  }
}
