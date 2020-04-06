/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.master.environment.app;

import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.customaction.AbstractCustomAction;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.workflow.AbstractWorkflow;

import java.util.Map;
import java.util.concurrent.Callable;

/**
 * An app with a workflow that just writes a single tracer value. Used to test preview.
 */
public class PreviewTestAppWithPlugin extends AbstractApplication<PreviewTestAppWithPlugin.Conf> {
  public static final String TRACER_NAME = "trace";
  public static final String TRACER_KEY = "k";
  public static final String PLUGIN_TYPE = "callable";
  public static final String PLUGIN_ID = "id";

  @Override
  public void configure() {
    addWorkflow(new TestWorkflow());
    Conf conf = getConfig();
    usePlugin(PLUGIN_TYPE, conf.pluginName, PLUGIN_ID,
              PluginProperties.builder().addAll(conf.pluginProperties).build());
  }

  /**
   * Config for the app
   */
  public static class Conf extends Config {
    private final String pluginName;

    private final Map<String, String> pluginProperties;

    public Conf(String pluginName, Map<String, String> pluginProperties) {
      this.pluginName = pluginName;
      this.pluginProperties = pluginProperties;
    }
  }

  /**
   * Workflow that has a single action that writes to a tracer.
   */
  public static class TestWorkflow extends AbstractWorkflow {
    public static final String NAME = TestWorkflow.class.getSimpleName();

    @Override
    protected void configure() {
      setName(NAME);
      addAction(new TracerWriter());
    }
  }

  /**
   * Writes a value to the tracer.
   */
  public static class TracerWriter extends AbstractCustomAction {

    @Override
    public void run() throws Exception {
      Callable<String> plugin = getContext().newPluginInstance(PLUGIN_ID);
      getContext().getDataTracer(TRACER_NAME).info(TRACER_KEY, plugin.call());
    }
  }
}
