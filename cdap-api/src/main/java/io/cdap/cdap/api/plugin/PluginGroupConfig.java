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

package io.cdap.cdap.api.plugin;

import io.cdap.cdap.api.Config;

/**
 * Config that can be defined as a field inside {@link PluginConfig} to represent a collection of configs.
 * When the plugin is deployed, the configs inside this class will be inspected and represented in the same way as
 * {@link PluginPropertyField}.
 * The {@link PluginPropertyField} for this field will contain a collection of property names about configs inside
 * this class.
 */
public class PluginGroupConfig extends Config {
}
