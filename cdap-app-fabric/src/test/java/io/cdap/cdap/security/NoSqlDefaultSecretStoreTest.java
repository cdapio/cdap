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

package io.cdap.cdap.security;

import com.google.inject.Injector;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.data.security.DefaultSecretStore;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Tests for {@link DefaultSecretStore} with NoSql storage implementation.
 */
public class NoSqlDefaultSecretStoreTest extends DefaultSecretStoreTest {
  @BeforeClass
  public static void setup() {
    Injector injector = AppFabricTestHelper.getInjector(CConfiguration.create());

    TransactionRunner transactionRunner = injector.getInstance(TransactionRunner.class);
    store = new DefaultSecretStore(transactionRunner);
  }

  @AfterClass
  public static void tearDown() {
    AppFabricTestHelper.shutdown();
  }
}
