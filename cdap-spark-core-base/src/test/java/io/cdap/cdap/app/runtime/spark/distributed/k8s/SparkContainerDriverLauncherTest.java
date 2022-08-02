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

package io.cdap.cdap.app.runtime.spark.distributed.k8s;

import com.google.inject.Injector;
import io.cdap.cdap.app.runtime.spark.MockMasterEnvironment;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.http.CommonNettyHttpServiceFactory;
import io.cdap.cdap.master.environment.MasterEnvironments;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 * Tests for SparkContainerDriverLauncher.
 */
public class SparkContainerDriverLauncherTest {
  @Test
  public void testInjectorSuccessfullyInjectsService() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    Configuration hConf = new Configuration();
    MasterEnvironment mockMasterEnvironment = new MockMasterEnvironment();
    MasterEnvironmentContext context = MasterEnvironments.createContext(cConf, hConf, "mock");
    mockMasterEnvironment.initialize(context);
    Injector injector = SparkContainerDriverLauncher.createInjector(cConf, hConf, mockMasterEnvironment);
    injector.getInstance(CommonNettyHttpServiceFactory.class);
  }
}
