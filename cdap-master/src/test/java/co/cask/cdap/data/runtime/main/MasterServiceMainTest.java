/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

package co.cask.cdap.data.runtime.main;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.zookeeper.election.LeaderElectionInfoService;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.internal.app.services.AppFabricServer;
import co.cask.cdap.security.authorization.AuthorizerInstantiator;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.multibindings.MapBinder;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class MasterServiceMainTest {

  @Test
  public void testInjector() {
    Injector baseInjector = MasterServiceMain.createProcessInjector(CConfiguration.create(), new Configuration());
    Injector injector = MasterServiceMain.createLeaderInjector(
      baseInjector.getInstance(CConfiguration.class),
      baseInjector.getInstance(Configuration.class),
      baseInjector.getInstance(ZKClientService.class),
      new LeaderElectionInfoService(baseInjector.getInstance(ZKClientService.class), "/election")
    );

    Assert.assertNotNull(injector.getInstance(AuthorizerInstantiator.class));
    Assert.assertNotNull(injector.getInstance(DatasetService.class));
    Assert.assertNotNull(injector.getInstance(AppFabricServer.class));
  }

  @Test
  public void test() {
    Guice.createInjector(new AbstractModule() {
      @Override
      protected void configure() {
        MapBinder<String, String> mapBinder = MapBinder.newMapBinder(
          binder(), String.class, String.class, Constants.Dataset.Manager.DefaultDatasetModules.class);
        mapBinder.addBinding("key1").toInstance("value1");
      }
    }, new AbstractModule() {
      @Override
      protected void configure() {
        MapBinder<String, String> mapBinder = MapBinder.newMapBinder(
          binder(), String.class, String.class, Constants.Dataset.Manager.DefaultDatasetModules.class);
        mapBinder.addBinding("key2").toInstance("value2");
      }
    });
  }
}
