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

package io.cdap.cdap.app.runtime.spark;

import com.google.inject.Injector;
import io.cdap.cdap.app.runtime.ProgramRuntimeProvider;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.app.worker.ConfiguratorTask;
import io.cdap.cdap.internal.app.worker.SystemAppTask;
import io.cdap.cdap.master.environment.MasterEnvironments;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.proto.ProgramType;
import org.junit.Test;

import java.util.ServiceLoader;

/**
 * Test for {@link SparkProgramRuntimeProvider}.
 */
public class SparkProgramRuntimeProviderTest {

  @Test
  public void testConfiguratorTaskInjector() throws Exception {
    MasterEnvironment masterEnvironment = new MockMasterEnvironment();
    masterEnvironment.initialize(null);
    MasterEnvironment tmpMasterEnv = MasterEnvironments.setMasterEnvironment(masterEnvironment);

    CConfiguration cConf = CConfiguration.create();
    Injector injector = ConfiguratorTask.createInjector(cConf);
    ServiceLoader<ProgramRuntimeProvider> serviceLoader = ServiceLoader.load(ProgramRuntimeProvider.class);
    SparkProgramRuntimeProvider runtimeProvider = (SparkProgramRuntimeProvider) serviceLoader.iterator().next();
    runtimeProvider.createProgramClassLoader(cConf, ProgramType.SPARK);

    MasterEnvironments.setMasterEnvironment(tmpMasterEnv);
  }

  @Test
  public void testSystemAppTaskInjector() throws Exception {
    MasterEnvironment masterEnvironment = new MockMasterEnvironment();
    masterEnvironment.initialize(null);
    MasterEnvironment tmpMasterEnv = MasterEnvironments.setMasterEnvironment(masterEnvironment);

    CConfiguration cConf = CConfiguration.create();
    Injector injector = SystemAppTask.createInjector(cConf);
    ServiceLoader<ProgramRuntimeProvider> serviceLoader = ServiceLoader.load(ProgramRuntimeProvider.class);
    SparkProgramRuntimeProvider runtimeProvider = (SparkProgramRuntimeProvider) serviceLoader.iterator().next();
    runtimeProvider.createProgramClassLoader(cConf, ProgramType.SPARK);

    MasterEnvironments.setMasterEnvironment(tmpMasterEnv);
  }

}
