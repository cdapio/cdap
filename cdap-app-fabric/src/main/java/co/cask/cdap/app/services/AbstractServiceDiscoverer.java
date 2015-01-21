/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.app.services;

import co.cask.cdap.api.ServiceDiscoverer;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.ServiceDiscoveries;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 * An abstract implementation of {@link ServiceDiscoverer}.
 * It provides definition for {@link ServiceDiscoverer#getServiceURL}  and expects the sub-classes to give definition
 * for {@link AbstractServiceDiscoverer#getDiscoveryServiceClient}.
 */
public abstract class AbstractServiceDiscoverer implements ServiceDiscoverer {

  protected String namespaceId;
  protected String applicationId;

  protected AbstractServiceDiscoverer() {
  }

  public AbstractServiceDiscoverer(Program program) {
    this.namespaceId = program.getNamespaceId();
    this.applicationId = program.getApplicationId();
  }

  @Override
  public URL getServiceURL(final String applicationId, final String serviceId) {
    String discoveryName = String.format("service.%s.%s.%s", namespaceId, applicationId, serviceId);
    String path = String.format("%s/namespaces/%s/apps/%s/services/%s/methods/",
                                Constants.Gateway.API_VERSION_3, namespaceId, applicationId, serviceId);
    return ServiceDiscoveries.discoverURL(getDiscoveryServiceClient(),
                                          discoveryName, "http", path, 1, TimeUnit.SECONDS);
  }

  @Override
  public URL getServiceURL(String serviceId) {
    return getServiceURL(applicationId, serviceId);
  }

  /**
   * @return the {@link DiscoveryServiceClient} for Service Discovery
   */
  protected abstract DiscoveryServiceClient getDiscoveryServiceClient();
}
