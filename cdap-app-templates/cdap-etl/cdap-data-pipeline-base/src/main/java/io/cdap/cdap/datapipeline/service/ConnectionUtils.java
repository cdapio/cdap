/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.datapipeline.service;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.macro.MacroParserOptions;
import io.cdap.cdap.api.plugin.InvalidPluginConfigException;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.service.http.ServicePluginConfigurer;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.common.BasicArguments;
import io.cdap.cdap.etl.common.DefaultMacroEvaluator;
import io.cdap.cdap.etl.common.OAuthMacroEvaluator;
import io.cdap.cdap.etl.common.SecureStoreMacroEvaluator;
import io.cdap.cdap.etl.proto.connection.ConnectionBadRequestException;
import io.cdap.cdap.etl.proto.connection.PluginInfo;
import io.cdap.cdap.etl.spec.TrackedPluginSelector;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

public class ConnectionUtils {

  private ConnectionUtils() {
    // prevent instantiation of util class.
  }

  public static Connector getConnector(ServicePluginConfigurer configurer, PluginInfo pluginInfo,
                                       TrackedPluginSelector pluginSelector, MacroEvaluator macroEvaluator,
                                       MacroParserOptions options) {
    Connector connector = null;
    try {
      connector = configurer.usePlugin(pluginInfo.getType(), pluginInfo.getName(), UUID.randomUUID().toString(),
                                       PluginProperties.builder().addAll(pluginInfo.getProperties()).build(),
                                       pluginSelector, macroEvaluator, options);
    } catch (InvalidPluginConfigException e) {
      throw new ConnectionBadRequestException(
        String.format("Unable to instantiate connector plugin: %s", e.getMessage()), e);
    }

    if (connector == null) {
      throw new ConnectionBadRequestException(String.format("Unable to find connector '%s'", pluginInfo.getName()));
    }
    return connector;
  }
}
