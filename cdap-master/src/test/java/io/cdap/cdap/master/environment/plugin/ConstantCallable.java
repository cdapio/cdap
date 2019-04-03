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

package co.cask.cdap.master.environment.plugin;

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.master.environment.ServiceWithPluginApp;

import java.util.concurrent.Callable;

/**
 * Used to test plugins in apps.
 */
@Plugin(type = ServiceWithPluginApp.PLUGIN_TYPE)
@Name(ConstantCallable.NAME)
public class ConstantCallable implements Callable<String> {
  public static final String NAME = "constant";
  private final Conf conf;

  public ConstantCallable(Conf conf) {
    this.conf = conf;
  }

  @Override
  public String call() throws Exception {
    return conf.val;
  }

  public static class Conf extends PluginConfig {
    private String val;
  }
}
