/*
 * Copyright © 2021 Cask Data, Inc.
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

import com.google.gson.Gson;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginConfigurer;
import io.cdap.cdap.service.DynamicPluginServiceApp;
import java.util.Map;
import java.util.function.Function;

/**
 * Plugin Function that returns raw properties and conf as response
 */
@Plugin(type = DynamicPluginServiceApp.PLUGIN_TYPE)
@Name(MacroFunction.NAME)
public class MacroFunction implements Function<PluginConfigurer, String> {
  public static final String NAME = "macro";
  private static final Gson GSON = new Gson();
  private final Conf conf;

  public MacroFunction(Conf conf) {
    this.conf = conf;
  }

  @Override
  public String apply(PluginConfigurer pluginConfigurer) {
    return GSON.toJson(new MacroFunctionResult(conf.getRawProperties().getProperties(), conf));
  }

  /**
   * Config for the supplier
   */
  public static class Conf extends PluginConfig {
    @Macro
    private String key1;

    @Macro
    private String key2;

    public String getKey1() {
      return key1;
    }

    public String getKey2() {
      return key2;
    }
  }

  public static class MacroFunctionResult {
    private final Map<String, String> rawProperties;
    private final Conf conf;

    public MacroFunctionResult(Map<String, String> rawProperties, Conf conf) {
      this.rawProperties = rawProperties;
      this.conf = conf;
    }

    public Map<String, String> getRawProperties() {
      return rawProperties;
    }

    public Conf getConf() {
      return conf;
    }
  }
}
