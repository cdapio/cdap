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

package co.cask.cdap.internal.app.runtime.artifact.app.inspection;

import co.cask.cdap.api.Config;
import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.plugin.PluginConfig;

/**
 * App used in artifact inspector tests
 */
public class InspectionApp extends AbstractApplication<InspectionApp.AConfig> {
  public static final String PLUGIN_DESCRIPTION = "some plugin";
  public static final String PLUGIN_NAME = "pluginA";
  public static final String PLUGIN_TYPE = "A";

  public static class AConfig extends Config {
    private int x;
    private String str;
  }

  public static class PConfig extends PluginConfig {
    private double y;
    private boolean isSomething;
  }

  @Override
  public void configure() {
    // nothing since its not a real app
  }

  @Plugin(type = PLUGIN_TYPE)
  @Name(PLUGIN_NAME)
  @Description(PLUGIN_DESCRIPTION)
  public static class AppPlugin {
    private PConfig pluginConf;

    public double doSomething() {
      return pluginConf.y;
    }
  }
}
