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

package co.cask.cdap.explore.client;

import java.net.InetSocketAddress;
import javax.annotation.Nullable;

/**
 * An Explore Client that uses the provided host and port to talk to a server
 * implementing {@link co.cask.cdap.explore.service.Explore} over HTTP.
 */
public class FixedAddressExploreClient extends AbstractExploreClient {
  private final InetSocketAddress addr;
  private final String authToken;

  public FixedAddressExploreClient(String host, int port, @Nullable String authToken) {
    this.addr = InetSocketAddress.createUnresolved(host, port);
    this.authToken = authToken;
  }

  @Override
  protected InetSocketAddress getExploreServiceAddress() {
    return addr;
  }

  protected String getAuthorizationToken() {
    return authToken;
  }
}
