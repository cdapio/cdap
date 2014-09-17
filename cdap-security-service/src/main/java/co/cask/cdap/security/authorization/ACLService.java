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

package co.cask.cdap.security.authorization;

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.ACLTable;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.http.HttpHandler;
import co.cask.http.NettyHttpService;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import javax.inject.Inject;

/**
 * Exposes the system {@link ACLTable} via REST endpoints using {@link ACLHandler}.
 */
public class ACLService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(ACLService.class);
  private static final String ACL_TABLE_NAME = "acls";

  private final DatasetFramework dsFramework;
  private final DiscoveryService discoveryService;
  private final DiscoveryServiceClient discoveryClient;

  private NettyHttpService service;
  private Cancellable cancelDiscovery;

  @Inject
  public ACLService(DatasetFramework dsFramework, DiscoveryService discoveryService,
                    DiscoveryServiceClient discoveryClient) {
    this.dsFramework = dsFramework;
    this.discoveryService = discoveryService;
    this.discoveryClient = discoveryClient;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting ACLService");

    // TODO: don't use dsFramework until dataset service is available
    ACLTable aclTable = DatasetsUtil.getOrCreateDataset(dsFramework, ACL_TABLE_NAME, ACLTable.class.getName(),
                                                        DatasetProperties.EMPTY, null, null);

    service = NettyHttpService.builder()
      .addHttpHandlers(ImmutableList.<HttpHandler>of(new ACLHandler(aclTable)))
      .build();
    service.startAndWait();

    cancelDiscovery = discoveryService.register(new Discoverable() {
      @Override
      public String getName() {
        return Constants.Service.ACL;
      }

      @Override
      public InetSocketAddress getSocketAddress() {
        return service.getBindAddress();
      }
    });

    LOG.info("Started ACLService");
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping ACLService");

    cancelDiscovery.cancel();
    service.stopAndWait();

    LOG.info("Stopped ACLService");
  }
}
