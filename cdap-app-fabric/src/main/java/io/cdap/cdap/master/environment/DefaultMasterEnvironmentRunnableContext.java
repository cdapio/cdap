/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.master.environment;

import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentRunnableContext;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;
import java.net.HttpURLConnection;

/**
 * Default implementation of {@link MasterEnvironmentRunnableContext}.
 */
public class DefaultMasterEnvironmentRunnableContext implements MasterEnvironmentRunnableContext {
  private final DiscoveryServiceClient discoveryServiceClient;
  private final LocationFactory locationFactory;
  private final RemoteClient remoteClient;

  public DefaultMasterEnvironmentRunnableContext(DiscoveryServiceClient discoveryServiceClient,
                                                 LocationFactory locationFactory) {
    this.discoveryServiceClient = discoveryServiceClient;
    this.locationFactory = locationFactory;
    this.remoteClient = new RemoteClient(discoveryServiceClient, Constants.Service.APP_FABRIC_HTTP,
                                         new DefaultHttpRequestConfig(false), "");
  }

  @Override
  public LocationFactory getLocationFactory() {
    return locationFactory;
  }

  /**
   * Opens a {@link HttpURLConnection} for the given resource path.
   */
  @Override
  public HttpURLConnection openHttpURLConnection(String resource) throws IOException {
    return remoteClient.openConnection(resource);
  }
}
