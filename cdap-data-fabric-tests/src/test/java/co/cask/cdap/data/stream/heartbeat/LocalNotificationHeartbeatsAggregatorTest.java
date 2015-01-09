/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data.stream.heartbeat;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.data.runtime.DataFabricLevelDBModule;
import co.cask.cdap.data.runtime.TransactionMetricsModule;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 *
 */
public class LocalNotificationHeartbeatsAggregatorTest extends NotificationHeartbeatsAggregatorTestBase {

  private static StreamAdmin streamAdmin;

  @BeforeClass
  public static void init() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, tmpFolder.newFolder().getAbsolutePath());

    Injector injector = createInjector(
      cConf, new Configuration(),
      new LocationRuntimeModule().getInMemoryModules(),
      new DataFabricLevelDBModule(),
      new TransactionMetricsModule()
    );

    startServices(injector);
    streamAdmin = injector.getInstance(StreamAdmin.class);
  }

  @AfterClass
  public static void shutDown() throws Exception {
    stopServices();
  }

  @Override
  protected StreamAdmin getStreamAdmin() {
    return streamAdmin;
  }

}
