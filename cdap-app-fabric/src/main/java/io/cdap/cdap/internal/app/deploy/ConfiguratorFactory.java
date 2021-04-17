/*
 * Copyright © 2021 Cask Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package io.cdap.cdap.internal.app.deploy;

import com.google.inject.Inject;
import io.cdap.cdap.app.deploy.Configurator;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;

public class ConfiguratorFactory {

  @Inject
  private DiscoveryServiceClient discoveryServiceClient;
  private final boolean isRemote;

  @Inject
  public ConfiguratorFactory(boolean isRemote) {
    this.isRemote = isRemote;
  }

  public Configurator build(CConfiguration cConf, Id.Namespace appNamespace, Id.Artifact artifactId,
                            String appClassName, PluginFinder pluginFinder,
                            ClassLoader artifactClassLoader,
                            String applicationName, String applicationVersion,
                            String configString, Location artifactLocation){
    if (isRemote){
      return new RemoteConfigurator(cConf, appNamespace, artifactId,
                                    appClassName, pluginFinder,
                                    artifactClassLoader,
                                    applicationName, applicationVersion,
                                    configString, this.discoveryServiceClient, artifactLocation);
    }else{
      return new InMemoryConfigurator(cConf, appNamespace, artifactId,
                                      appClassName, pluginFinder,
                                      artifactClassLoader,
                                      applicationName, applicationVersion,
                                      configString);
    }
  }
}
