/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.store.state;

import com.google.inject.Injector;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import org.apache.tephra.TransactionManager;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

public class AppStateTableTest extends AppFabricTestBase {

  public static final String NAMESPACE_1 = "ns1";
  public static final String APP_NAME = "testapp";
  public static final String STATE_KEY = "kafka";
  public static final String STATE_KEY_2 = "pubSub";
  public static final byte[] STATE_VALUE = ("{\n" +
                                            "\"offset\" : 12345\n" +
                                            "}").getBytes(StandardCharsets.UTF_8);

  private static AppState request;
  private static AppState request2;
  private static AppStateTable appStateTable;
  private static TransactionManager txManager;
  private static TransactionRunner transactionRunner;


  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @BeforeClass
  public static void setup() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    Injector injector = getInjector();

    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();

    transactionRunner = getInjector().getInstance(TransactionRunner.class);

    request = new AppState(NAMESPACE_1, APP_NAME, STATE_KEY, STATE_VALUE);
    request2 = new AppState(NAMESPACE_1, APP_NAME, STATE_KEY_2, STATE_VALUE);
  }

  @AfterClass
  public static void teardown() throws Exception {
    if (txManager != null) {
      txManager.stopAndWait();
    }
  }

  @Before
  public void setUp() throws Exception {

  }

  @After
  public void tearDown() throws Exception {
    // Cleanup
    try {
      TransactionRunners.run(transactionRunner, context -> {
        getAppStateTable(context).deleteAllStates(request);
      });
    } catch (Exception e) {
      // Exception because state might already have been deleted.
      // Don't do anything.
    }

    appStateTable = null;
  }

  @Test
  public void testAppStateSaveAndGet() {
    Optional<byte[]> stateValue = TransactionRunners.run(transactionRunner, context -> {
      getAppStateTable(context).saveState(request);
      return getAppStateTable(context).getState(request);
    });

    Assert.assertTrue(stateValue.isPresent());
    Assert.assertArrayEquals(STATE_VALUE, stateValue.get());
  }

  @Test
  public void testAppStateDelete() {
    // Save state
    Optional<byte[]> stateValue = TransactionRunners.run(transactionRunner, context -> {
      getAppStateTable(context).saveState(request);
      getAppStateTable(context).deleteState(request);
      return getAppStateTable(context).getState(request);
    });

    Assert.assertFalse(stateValue.isPresent());
  }

  @Test
  public void testAppStateDeleteAll() {
    // Save state
    Optional<byte[]> stateValue = TransactionRunners.run(transactionRunner, context -> {
      getAppStateTable(context).saveState(request);
      getAppStateTable(context).saveState(request2);

      AppState deleteAllRequest = new AppState(NAMESPACE_1, APP_NAME, "");
      getAppStateTable(context).deleteAllStates(deleteAllRequest);
      return getAppStateTable(context).getState(request);
    });

    Assert.assertFalse(stateValue.isPresent());
  }

  private AppStateTable getAppStateTable(StructuredTableContext context) throws TableNotFoundException {
    if (appStateTable == null) {
      appStateTable = new AppStateTable(context);
    }
    return appStateTable;
  }
}
