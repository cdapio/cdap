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

package io.cdap.cdap.master.environment;

import com.google.inject.name.Named;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.CConfigurationUtil;
import io.cdap.cdap.master.environment.k8s.MasterEnvironmentMain;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentRunnable;
import org.apache.twill.filesystem.LocationFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;
import javax.inject.Inject;

/**
 * Default implementation of {@link MasterEnvironmentContext} that reflects the actual master runtime environment.
 */
public class DefaultMasterEnvironmentContext implements MasterEnvironmentContext {

  static final String MASTER_ENV_NAME = "masterEnvName";

  private final String masterEnvName;
  private final LocationFactory locationFactory;
  private final Map<String, String> configuration;

  @Inject
  DefaultMasterEnvironmentContext(CConfiguration cConf, LocationFactory locationFactory,
                                  @Named(MASTER_ENV_NAME) String masterEnvName) {
    this.masterEnvName = masterEnvName;
    this.locationFactory = locationFactory;
    this.configuration = CConfigurationUtil.asMap(cConf);
  }

  @Override
  public LocationFactory getLocationFactory() {
    return locationFactory;
  }

  @Override
  public Map<String, String> getConfigurations() {
    return configuration;
  }

  @Override
  public String[] getRunnableArguments(Class<? extends MasterEnvironmentRunnable> runnableClass,
                                       String... runnableArgs) {
    return Stream.concat(
      Stream.of(MasterEnvironmentMain.class.getName(),
                "--env=" + masterEnvName, "--runnableClass=" + runnableClass.getName()),
      Arrays.stream(runnableArgs)
    ).toArray(String[]::new);
  }
}
