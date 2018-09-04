/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.common.logging.LogSamplers;
import co.cask.cdap.common.logging.Loggers;
import co.cask.cdap.common.service.AbstractRetryableScheduledService;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.internal.app.store.DefaultStore;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.protobuf.Service;
import org.apache.tephra.TransactionNotInProgressException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link Service} that will start the upgrade threads of default store and also provides the status of upgrade.
 */
@Singleton
public class RunCountUpgradeService extends AbstractRetryableScheduledService {
  private static final Logger LOG = LoggerFactory.getLogger(RunCountUpgradeService.class);
  // For outage, only log once per 60 seconds per message.
  private static final Logger OUTAGE_LOG = Loggers.sampling(LOG, LogSamplers.perMessage(
    () -> LogSamplers.limitRate(60000)));

  private final DefaultStore defaultStore;
  // by default batch size is 1000
  private final AtomicInteger maxRows = new AtomicInteger(1000);


  @Inject
  RunCountUpgradeService(DefaultStore defaultStore) {
    super(RetryStrategies.exponentialDelay(1, 60, TimeUnit.SECONDS));
    this.defaultStore = defaultStore;
  }

  @Override
  protected void doStartUp() throws Exception {
    LOG.info("Starting Run Count Upgrade Service.");
  }

  @Override
  public long runTask() throws Exception {
    if (defaultStore.isUpgradeComplete()) {
      LOG.info("Run count upgrade completed.");
      stop();
      return 0;
    }

    try {
      defaultStore.upgrade(maxRows.get());
    } catch (Exception e) {
      // this means the batch size is too large and it causes tx timeout, reduce the batch size
      if (e.getCause() instanceof TransactionNotInProgressException) {
        int currMaxRows = maxRows.get();
        maxRows.set(Math.max(currMaxRows - 200, 100));
      }
      OUTAGE_LOG.warn("Error occurred while upgrading run count. " +
                        "Run counts may be incorrect until upgrade succeeds.", e);
      throw e;
    }

    return 0;
  }

  @Override
  protected String getServiceName() {
    return "run-count-upgrade-service";
  }

  @Override
  protected void doShutdown() throws Exception {
    LOG.info("Stopping Run Count Upgrade Service.");
  }
}
