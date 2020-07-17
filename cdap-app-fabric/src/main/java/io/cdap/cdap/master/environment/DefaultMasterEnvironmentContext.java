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

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.CConfigurationUtil;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import org.apache.twill.filesystem.LocationFactory;

import java.util.Map;
import javax.inject.Inject;

/**
 * Default implementation of {@link MasterEnvironmentContext} that reflects the actual master runtime environment.
 */
public class DefaultMasterEnvironmentContext implements MasterEnvironmentContext {

  private final LocationFactory locationFactory;
  private final Map<String, String> configuration;

  @Inject
  DefaultMasterEnvironmentContext(LocationFactory locationFactory, CConfiguration cConf) {
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
}
