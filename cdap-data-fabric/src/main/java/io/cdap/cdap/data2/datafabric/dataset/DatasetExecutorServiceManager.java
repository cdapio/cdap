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

package co.cask.cdap.data2.datafabric.dataset;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.twill.AbstractDistributedMasterServiceManager;
import com.google.inject.Inject;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.discovery.DiscoveryServiceClient;

/**
 * CDAP Dataset Service management in distributed mode.
 */
public class DatasetExecutorServiceManager extends AbstractDistributedMasterServiceManager {

  @Inject
  public DatasetExecutorServiceManager(CConfiguration cConf, TwillRunnerService twillRunnerService,
                                       DiscoveryServiceClient discoveryServiceClient) {
    super(cConf, Constants.Service.DATASET_EXECUTOR, twillRunnerService, discoveryServiceClient);
    this.discoveryServiceClient = discoveryServiceClient;
  }

  @Override
  public int getMaxInstances() {
    return cConf.getInt(Constants.Dataset.Executor.MAX_INSTANCES);
  }

  @Override
  public String getDescription() {
    return Constants.Dataset.Executor.SERVICE_DESCRIPTION;
  }

}
