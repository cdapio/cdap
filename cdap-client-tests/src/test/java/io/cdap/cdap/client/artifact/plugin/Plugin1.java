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

package co.cask.cdap.client.artifact.plugin;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.plugin.PluginConfig;

import java.util.concurrent.Callable;

/**
 * Plugin for ArtifactClient test
 */
@Plugin(type = "callable")
@Name("plugin1")
@Description("p1 description")
public class Plugin1 implements Callable<Integer> {
  private P1Config conf;

  @Override
  public Integer call() throws Exception {
    return conf.x;
  }

  public static class P1Config extends PluginConfig {
    private int x;
  }

}
