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

package co.cask.cdap.internal.app.runtime.batch;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.twill.AbstractInMemoryMasterServiceManager;
import co.cask.tephra.TransactionSystemClient;
import com.google.inject.Inject;

/**
 *
 */
public class InMemoryTransactionServiceManager extends AbstractInMemoryMasterServiceManager {
  private TransactionSystemClient txClient;

  @Override
  public boolean isLogAvailable() {
    return false;
  }

  @Inject
  public InMemoryTransactionServiceManager(TransactionSystemClient txClient) {
    this.txClient = txClient;
  }

  @Override
  public boolean isServiceAvailable() {
    return txClient.status().equals(Constants.Monitor.STATUS_OK);
  }

  @Override
  public String getDescription() {
    return Constants.Transaction.SERVICE_DESCRIPTION;
  }

}
