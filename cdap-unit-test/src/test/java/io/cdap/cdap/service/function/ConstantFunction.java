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
 *
 */

package io.cdap.cdap.service.function;

import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginConfigurer;
import io.cdap.cdap.service.DynamicPluginServiceApp;

import java.util.function.Function;

/**
 * Plugin Function that just returns whatever value it was configured to return.
 */
@Plugin(type = DynamicPluginServiceApp.PLUGIN_TYPE)
@Name(ConstantFunction.NAME)
public class ConstantFunction implements Function<PluginConfigurer, String> {
  public static final String NAME = "constant";
  private final Conf conf;

  public ConstantFunction(Conf conf) {
    this.conf = conf;
  }

  @Override
  public String apply(PluginConfigurer pluginConfigurer) {
    return conf.value;
  }

  /**
   * Config for the supplier
   */
  public static class Conf extends PluginConfig {
    private String value;
  }
}
