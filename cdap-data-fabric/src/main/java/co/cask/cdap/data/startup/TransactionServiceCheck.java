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

package co.cask.cdap.data.startup;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.startup.Check;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TxConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Startup check that validates the configuration of the transaction service.
 */
public class TransactionServiceCheck extends Check {

  private static final Logger LOG = LoggerFactory.getLogger(TransactionServiceCheck.class);

  private final CConfiguration cConf;
  private final Configuration hConf;

  @Inject
  public TransactionServiceCheck(CConfiguration cConf) {
    this.cConf = cConf;
    this.hConf = null;
  }

  public TransactionServiceCheck(Configuration hConf) {
    this.cConf = null;
    this.hConf = hConf;
  }

  @Override
  public void run() throws Exception {
    LOG.info("Validating transaction service configuration.");
    int defaultTxTimeout = getConfiguredInteger(TxConstants.Manager.CFG_TX_TIMEOUT, "Default transaction timeout");
    int maxTxTimeout =  getConfiguredInteger(TxConstants.Manager.CFG_TX_MAX_TIMEOUT, "Transaction timeout limit");
    if (defaultTxTimeout > maxTxTimeout) {
      throw new IllegalArgumentException(String.format(
        "Default transaction timeout (%s) of %d seconds must not exceed the transaction timeout limit (%s) of %d",
        TxConstants.Manager.CFG_TX_TIMEOUT, defaultTxTimeout, TxConstants.Manager.CFG_TX_MAX_TIMEOUT, maxTxTimeout));
    }
    LOG.info("Transaction service configuration successfully validated.");
  }

  private int getConfiguredInteger(String propertyName, String description) {
    try {
      return getConfiguredInteger(propertyName);
    } catch (NullPointerException e) {
      throw new IllegalArgumentException(String.format("%s (%s) is not configured.", description, propertyName));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(String.format("%s (%s) is not a number.", description, propertyName), e);
    }
  }

  private int getConfiguredInteger(String propertyName) {
    if (cConf != null) {
      return cConf.getInt(propertyName);
    } else if (hConf != null) {
      if (hConf.get(propertyName) != null) {
        return hConf.getInt(propertyName, 0);
      }
      throw new NullPointerException();
    }
    // this should never happen because the constructors ensure either cConf or hConf is not null
    throw new NullPointerException();
  }

}
