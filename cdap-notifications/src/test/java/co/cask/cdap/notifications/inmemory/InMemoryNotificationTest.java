/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.notifications.inmemory;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.notifications.NotificationTest;
import co.cask.cdap.notifications.feeds.guice.NotificationFeedServiceRuntimeModule;
import co.cask.cdap.notifications.guice.NotificationServiceRuntimeModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Tests notifications using in-memory transport system.
 */
public class InMemoryNotificationTest extends NotificationTest {

  @BeforeClass
  public static void start() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    Injector injector = Guice.createInjector(
      Iterables.concat(
        getCommonModules(),
        ImmutableList.of(
          new ConfigModule(cConf),
          new NotificationServiceRuntimeModule().getInMemoryModules(),
          new NotificationFeedServiceRuntimeModule().getInMemoryModules()
        )
      )
    );
    startServices(injector);
  }

  @AfterClass
  public static void shutDown() throws Exception {
    stopServices();
  }
}
