/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.spark.app.plugin;

import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.api.plugin.PluginConfigurer;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.spark.app.Extensible;

import java.util.function.ToIntFunction;

/**
 * A function that loads a {@link UDT} to do the job.
 */
@Plugin(type = "function")
@Name("pluggable")
public class PluggableFunc implements ToIntFunction<String>, Extensible {

  private final Config config;
  private UDT udt;

  public PluggableFunc(Config config) {
    this.config = config;
  }

  @Override
  public int applyAsInt(String value) {
    return udt.apply(value);
  }

  @Override
  public void configure(PluginConfigurer configurer) {
    configurer.usePluginClass("udt", config.udtName, "udt", PluginProperties.builder().build());
  }

  @Override
  public void initialize(PluginContext context) throws Exception {
    udt = context.newPluginInstance("udt");
  }

  private static final class Config extends PluginConfig {

    @Macro
    private final String udtName;

    private Config(String udtName) {
      this.udtName = udtName;
    }
  }
}
