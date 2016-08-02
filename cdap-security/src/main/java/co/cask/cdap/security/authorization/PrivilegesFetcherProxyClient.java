/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.security.authorization;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import com.google.inject.Inject;
import org.apache.twill.discovery.DiscoveryServiceClient;

/**
 * A {@link AbstractPrivilegesFetcherClient} to proxy requests to list privileges from program containers and system
 * services to the master (appfabric). It runs inside a dedicated service (RemoteSystemOperationsService) and needs to
 * proxy requests to the master because non-master services may not have access to authorization backends, since they
 * do not have the Kerberos credentials of the master.
 */
class PrivilegesFetcherProxyClient extends AbstractPrivilegesFetcherClient {

  @Inject
  PrivilegesFetcherProxyClient(CConfiguration cConf, final DiscoveryServiceClient discoveryClient) {
    super(cConf, discoveryClient, Constants.Service.APP_FABRIC_HTTP);
  }
}
