/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.distributed;

import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.twill.AbstractMasterServiceManager;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transaction Service Management in Distributed Mode.
 */
public class TransactionServiceManager extends AbstractMasterServiceManager {

  private static final Logger LOG = LoggerFactory.getLogger(TransactionServiceManager.class);

  private final TransactionSystemClient txClient;

  @Inject
  TransactionServiceManager(CConfiguration cConf, TwillRunner twillRunner,
                            TransactionSystemClient txClient, DiscoveryServiceClient discoveryClient) {
    super(cConf, discoveryClient, Constants.Service.TRANSACTION, twillRunner);
    this.txClient = txClient;
  }

  @Override
  public boolean isServiceEnabled() {
    return getCConf().getBoolean(Constants.Transaction.TX_ENABLED);
  }

  @Override
  public int getMaxInstances() {
    return getCConf().getInt(Constants.Transaction.Container.MAX_INSTANCES);
  }

  @Override
  public boolean isServiceAvailable() {
    if (!isServiceEnabled()) {
      return false;
    }
    try {
      return txClient.status().equals(Constants.Monitor.STATUS_OK);
    } catch (Exception e) {
      LOG.warn("Unable to ping {}", Constants.Service.TRANSACTION, e);
      return false;
    }
  }

  @Override
  public String getDescription() {
    return Constants.Transaction.SERVICE_DESCRIPTION;
  }
}
