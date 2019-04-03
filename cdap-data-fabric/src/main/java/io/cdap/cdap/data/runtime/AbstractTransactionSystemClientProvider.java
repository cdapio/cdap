/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.data.runtime;

import com.google.inject.Provider;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import org.apache.tephra.TransactionSystemClient;

/**
 * Class to provide the {@link TransactionSystemClient}.
 */
abstract class AbstractTransactionSystemClientProvider implements Provider<TransactionSystemClient> {
  private final CConfiguration cConf;

  AbstractTransactionSystemClientProvider(CConfiguration cConf) {
    this.cConf = cConf;
  }

  @Override
  public TransactionSystemClient get() {
    if (cConf.getBoolean(Constants.Transaction.TX_ENABLED)) {
      return getTransactionSystemClient();
    }

    // return an constant transation client if tx is disabled
    return new ConstantTransactionSystemClient();
  }

  /**
   * @return the transaction system client for the transaction service
   */
  protected abstract TransactionSystemClient getTransactionSystemClient();
}
