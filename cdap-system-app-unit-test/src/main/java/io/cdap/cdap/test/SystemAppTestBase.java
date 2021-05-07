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

package io.cdap.cdap.test;

import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import org.junit.BeforeClass;

/**
 * Base class to inherit from for system application unit-tests.
 *
 * @see TestBase
 */
public class SystemAppTestBase extends TestBase {
  private static TransactionRunner transactionRunner;
  private static StructuredTableAdmin tableAdmin;

  @BeforeClass
  public static void setupClass() {
    if (isFirstInit()) {
      transactionRunner = injector.getInstance(TransactionRunner.class);
      tableAdmin = injector.getInstance(StructuredTableAdmin.class);
    }
  }

  public static TransactionRunner getTransactionRunner() {
    return transactionRunner;
  }

  public static StructuredTableAdmin getStructuredTableAdmin() {
    return tableAdmin;
  }
}
