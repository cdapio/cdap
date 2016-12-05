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

package co.cask.cdap.explore.service;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.twill.AbstractDistributedMasterServiceManager;
import com.google.inject.Inject;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.discovery.DiscoveryServiceClient;

/**
 * Service manager for explore service in distributed mode.
 */
public class ExploreServiceManager extends AbstractDistributedMasterServiceManager {

  @Inject
  public ExploreServiceManager(CConfiguration cConf, TwillRunnerService twillRunnerService,
                               DiscoveryServiceClient discoveryServiceClient) {
    super(cConf, Constants.Service.EXPLORE_HTTP_USER_SERVICE, twillRunnerService, discoveryServiceClient);
    this.discoveryServiceClient = discoveryServiceClient;
  }

  @Override
  public boolean isServiceEnabled() {
    return cConf.getBoolean(Constants.Explore.EXPLORE_ENABLED);
  }

  @Override
  public int getMaxInstances() {
    return 1; // max explore service container instances is 1 (non-configurable)
  }

  @Override
  public String getDescription() {
    return Constants.Explore.SERVICE_DESCRIPTION;
  }

}
