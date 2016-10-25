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

package org.apache.tephra.inmemory;

import org.apache.tephra.TransactionManager;
import org.apache.tephra.TransactionSystemClient;
import org.junit.Assert;

/**
 * Helper class to get the package local txManager from an InMemoryTxSystemClient.
 * TODO (CDAP-7358): remove this class once TEPHRA-182 is fixed.
 */
public class TxInMemory {

  public static TransactionManager getTransactionManager(TransactionSystemClient txClient) {
    Assert.assertTrue("Unexpected txClient of class " + txClient.getClass().getName(),
                      txClient instanceof InMemoryTxSystemClient);
    return ((InMemoryTxSystemClient) txClient).txManager;
  }
}
