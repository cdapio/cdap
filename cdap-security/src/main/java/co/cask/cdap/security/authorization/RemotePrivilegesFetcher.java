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
 * A {@link AbstractPrivilegesFetcherClient} to make requests to fetch privileges from program containers and system
 * services to a dedicated system service (RemoteSystemOperationsService)
 * Communication over HTTP is necessary because program containers, which use this class (and run as the user running
 * the program) may not be white-listed to make calls to authorization providers (like Apache Sentry).
 */
class RemotePrivilegesFetcher extends AbstractPrivilegesFetcherClient {

  @Inject
  RemotePrivilegesFetcher(CConfiguration cConf, DiscoveryServiceClient discoveryClient) {
    super(cConf, discoveryClient, Constants.Service.REMOTE_SYSTEM_OPERATION);
  }
}
