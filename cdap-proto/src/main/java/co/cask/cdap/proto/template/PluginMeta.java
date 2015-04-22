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

package co.cask.cdap.proto.template;

import co.cask.cdap.api.templates.plugins.PluginClass;
import co.cask.cdap.api.templates.plugins.PluginInfo;

/**
 * Contains basic information of a template plugin.
 */
public class PluginMeta {

  private final ApplicationTemplateMeta template;
  private final PluginInfo source;
  private final String type;
  private final String name;
  private final String description;

  public PluginMeta(ApplicationTemplateMeta template, PluginInfo pluginInfo, PluginClass pluginClass) {
    this.template = template;
    this.source = pluginInfo;
    this.type = pluginClass.getType();
    this.name = pluginClass.getName();
    this.description = pluginClass.getDescription();
  }

  public ApplicationTemplateMeta getTemplate() {
    return template;
  }

  public PluginInfo getSource() {
    return source;
  }

  public String getDescription() {
    return description;
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }
}
