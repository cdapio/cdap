/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.workflow.AbstractWorkflow;

/**
 * Application consisting of a plugin and a workflow.
 */
public class WorkflowAppWithPlugins extends AbstractApplication {

  public static final String NAME = "WorkflowAppWithPlugins";
  public static final String DESC = "Application which has a workflow and plugins";

  public static final String PLUGIN_DESCRIPTION = "test plugin";
  public static final String PLUGIN_NAME = "testplugin-1";
  public static final String PLUGIN_TYPE = "testplugin";

  @Override
  public void configure() {
    setName(NAME);
    setDescription(DESC);
    usePlugin(PLUGIN_TYPE, PLUGIN_NAME, "id",
        PluginProperties.builder().build());
    addWorkflow(new NoOpWorkflow());
  }

  /**
   * Dummy no-op workflow.
   */
  public static class NoOpWorkflow extends AbstractWorkflow {

    public static final String NAME = "NoOpWorkflow";

    @Override
    public void configure() {
      setName(NAME);
      setDescription("NoOp Workflow description");
    }
  }

  /**
   * Dummy no-op plugin.
   */
  @Plugin(type = PLUGIN_TYPE)
  @Name(PLUGIN_NAME)
  @Description(PLUGIN_DESCRIPTION)
  public static class AppPlugin {

  }
}
