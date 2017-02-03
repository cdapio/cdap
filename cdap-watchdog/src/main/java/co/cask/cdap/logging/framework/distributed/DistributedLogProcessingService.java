/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.logging.framework.distributed;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.resource.ResourceBalancerService;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.zookeeper.ZKClientService;

import java.util.Set;

/**
 * A {@link ResourceBalancerService} for log processing in distributed mode
 */
public class DistributedLogProcessingService extends ResourceBalancerService {

  private static final String SERVICE_NAME = "log.framework";

  private final CConfiguration cConf;

  protected DistributedLogProcessingService(CConfiguration cConf, ZKClientService zkClient,
                                            DiscoveryService discoveryService,
                                            DiscoveryServiceClient discoveryServiceClient) {
    super(SERVICE_NAME, cConf.getInt(Constants.Logging.NUM_PARTITIONS),
          zkClient, discoveryService, discoveryServiceClient);
    this.cConf = cConf;
  }

  @Override
  protected Service createService(Set<Integer> partitions) {



    return new AbstractIdleService() {
      @Override
      protected void startUp() throws Exception {

      }

      @Override
      protected void shutDown() throws Exception {

      }
    };
  }
}
