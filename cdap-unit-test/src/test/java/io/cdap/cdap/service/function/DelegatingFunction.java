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

import com.google.gson.Gson;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginConfigurer;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.internal.guava.reflect.TypeToken;
import io.cdap.cdap.service.DynamicPluginServiceApp;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

/**
 * Plugin Function that tests a plugin using another plugin.
 */
@Plugin(type = DynamicPluginServiceApp.PLUGIN_TYPE)
@Name(DelegatingFunction.NAME)
public class DelegatingFunction implements Function<PluginConfigurer, String> {
  public static final String NAME = "delegating";
  private final Conf conf;

  public DelegatingFunction(Conf conf) {
    this.conf = conf;
  }

  @Override
  public String apply(PluginConfigurer pluginConfigurer) {
    Function<PluginConfigurer, String> delegate = pluginConfigurer.usePlugin(DynamicPluginServiceApp.PLUGIN_TYPE,
                                                                             conf.delegateName,
                                                                             UUID.randomUUID().toString(),
                                                                             conf.getPluginProperties());
    return delegate.apply(pluginConfigurer);
  }

  /**
   * Config for the supplier
   */
  public static class Conf extends PluginConfig {
    private static final Gson GSON = new Gson();
    private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();

    private String delegateName;

    private String properties;

    private PluginProperties getPluginProperties() {
      Map<String, String> propertiesMap = GSON.fromJson(properties, MAP_TYPE);
      return PluginProperties.builder()
        .addAll(propertiesMap)
        .build();
    }
  }

}
