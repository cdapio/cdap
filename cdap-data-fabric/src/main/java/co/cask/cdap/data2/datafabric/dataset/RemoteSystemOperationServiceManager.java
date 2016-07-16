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

package co.cask.cdap.data2.datafabric.dataset;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import com.google.inject.Inject;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.discovery.DiscoveryServiceClient;

/**
 * CDAP Remote Sytem Operations Management in distributed mode.
 */
public class RemoteSystemOperationServiceManager extends DatasetExecutorServiceManager {

  @Inject
  RemoteSystemOperationServiceManager(CConfiguration cConf, TwillRunnerService twillRunnerService,
                                             DiscoveryServiceClient discoveryServiceClient) {
    super(cConf, twillRunnerService, discoveryServiceClient);
  }

  @Override
  public String getDescription() {
    return Constants.RemoteSystemOpService.SERVICE_DESCRIPTION;
  }

  @Override
  public String getDiscoverableName() {
    return Constants.Service.REMOTE_SYSTEM_OPERATION;
  }
}
