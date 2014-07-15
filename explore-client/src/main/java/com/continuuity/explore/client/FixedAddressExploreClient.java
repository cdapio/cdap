/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.explore.client;

import com.continuuity.explore.service.ExploreException;
import com.continuuity.explore.service.Handle;

import java.net.InetSocketAddress;

/**
 * An Explore Client that uses the provided host and port to talk to a server
 * implementing {@link com.continuuity.explore.service.Explore} over HTTP.
 */
public class FixedAddressExploreClient extends AbstractExploreClient {
  private final InetSocketAddress addr;

  public FixedAddressExploreClient(String host, int port) {
    addr = InetSocketAddress.createUnresolved(host, port);
  }

  @Override
  protected InetSocketAddress getExploreServiceAddress() {
    return addr;
  }

  @Override
  public Handle enableExplore(String datasetInstance) throws ExploreException {
    throw new ExploreException("This client does not allow to enable explore on datasets.");
  }

  @Override
  public Handle disableExplore(String datasetInstance) throws ExploreException {
    throw new ExploreException("This client does not allow to disable explore on datasets");
  }
}
