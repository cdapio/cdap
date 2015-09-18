/*
 * Copyright © 2015 Cask Data, Inc.
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
 * CDAP Metadata Service management in distributed mode. Since the metadata service runs in the same
 * container as the dataset executor service, this is same as DatasetExecutorServiceManager, except
 * we use a different discoverable name and description.
 */
public class MetadataServiceManager extends DatasetExecutorServiceManager {

  @Inject
  public MetadataServiceManager(CConfiguration cConf, TwillRunnerService twillRunnerService,
                                DiscoveryServiceClient discoveryServiceClient) {
    super(cConf, twillRunnerService, discoveryServiceClient);
  }

  @Override
  public String getDescription() {
    return Constants.Metadata.SERVICE_DESCRIPTION;
  }

  @Override
  protected String getDiscoverableName() {
    return Constants.Service.METADATA_SERVICE;
  }
}
