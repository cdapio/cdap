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

package co.cask.cdap.data.runtime.main;

import co.cask.cdap.app.guice.AppFabricServiceRuntimeModule;
import co.cask.cdap.app.guice.ProgramRunnerRuntimeModule;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.guice.TwillModule;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Names;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class MasterServiceMainTest {

  @Test
  public void testInjector() {
    Injector baseInjector = MasterServiceMain.createBaseInjector(CConfiguration.create(), new Configuration());
    Injector injector = MasterServiceMain.createChildInjector(baseInjector);
    
    Assert.assertNotNull(injector.getInstance(DatasetService.class));
  }

  @Test
  public void test() {
    Guice.createInjector(new AbstractModule() {
      @Override
      protected void configure() {
        MapBinder<String, String> mapBinder = MapBinder.newMapBinder(binder(), String.class, String.class,
                                                                     Names.named("defaultDatasetModules"));
        mapBinder.addBinding("key1").toInstance("value1");
      }
    }, new AbstractModule() {
      @Override
      protected void configure() {
        MapBinder<String, String> mapBinder = MapBinder.newMapBinder(binder(), String.class, String.class,
                                                                     Names.named("defaultDatasetModules"));
        mapBinder.addBinding("key2").toInstance("value2");
      }
    });
  }
}
