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

package co.cask.cdap.internal.app.runtime.distributed;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.common.twill.AbstractDistributedMasterServiceManager;
import co.cask.tephra.TransactionSystemClient;
import com.google.inject.Inject;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Transaction Service Management in Distributed Mode.
 */
public class TransactionServiceManager extends AbstractDistributedMasterServiceManager {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionServiceManager.class);
  private TransactionSystemClient txClient;
  private DiscoveryServiceClient discoveryServiceClient;

  @Inject
  public TransactionServiceManager(CConfiguration cConf, TwillRunnerService twillRunnerService,
                                   TransactionSystemClient txClient, DiscoveryServiceClient discoveryServiceClient) {
    super(cConf, Constants.Service.TRANSACTION, twillRunnerService, discoveryServiceClient);
    this.txClient = txClient;
    this.discoveryServiceClient = discoveryServiceClient;
  }

  @Override
  public int getMaxInstances() {
    return cConf.getInt(Constants.Transaction.Container.MAX_INSTANCES);
  }

  @Override
  public boolean isServiceAvailable() {
    try {
      ServiceDiscovered discovered = discoveryServiceClient.discover(serviceName);
      Discoverable discoverable = new RandomEndpointStrategy(discovered).pick(discoveryTimeout, TimeUnit.SECONDS);
      if (discoverable == null) {
        return false;
      }

      return txClient.status().equals(Constants.Monitor.STATUS_OK);
    } catch (IllegalArgumentException e) {
      return false;
    } catch (Exception e) {
      LOG.warn("Unable to ping {} : Reason {} ", serviceName, e.getMessage());
      return false;
    }
  }

  @Override
  public String getDescription() {
    return Constants.Transaction.SERVICE_DESCRIPTION;
  }
}
