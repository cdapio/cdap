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
import co.cask.cdap.api.templates.plugins.PluginPropertyField;

import java.util.Map;

/**
 * Contains detail information of a template plugin.
 */
public class PluginDetail extends PluginMeta {

  private final String className;
  private final Map<String, PluginPropertyField> properties;

  public PluginDetail(ApplicationTemplateMeta template, PluginInfo pluginInfo, PluginClass pluginClass) {
    super(template, pluginInfo, pluginClass);
    this.className = pluginClass.getClassName();
    this.properties = pluginClass.getProperties();
  }

  public String getClassName() {
    return className;
  }

  public Map<String, PluginPropertyField> getProperties() {
    return properties;
  }
}
