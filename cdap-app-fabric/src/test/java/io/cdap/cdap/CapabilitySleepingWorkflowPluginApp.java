/*
 * Copyright © 2014-2020 Cask Data, Inc.
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

import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.annotation.Requirements;
import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.customaction.AbstractCustomAction;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.workflow.AbstractWorkflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Simple workflow, that sleeps inside a CustomAction
 */
public class CapabilitySleepingWorkflowPluginApp extends AbstractApplication<CapabilitySleepingWorkflowPluginApp.Conf> {

  public static final String NAME = "CapabilitySleepingWorkflowPluginApp";
  public static final String DESC = "CapabilitySleepingWorkflowPluginApp";

  @Override
  public void configure() {
    setName(NAME);
    setDescription(DESC);
    Conf conf = getConfig();
    usePlugin("simple", conf.pluginName, "id",
              PluginProperties.builder().addAll(conf.pluginProperties).build());
    addWorkflow(new SleepWorkflow());
  }

  /**
   * Config for the plugin
   */
  public static class Conf extends Config {
    private final String pluginName;

    private final Map<String, String> pluginProperties;

    public Conf() {
      this.pluginName = "SimpleRunnablePlugin";
      this.pluginProperties = new HashMap<>();
    }
  }

  /**
   * SleepWorkflow
   */
  public static class SleepWorkflow extends AbstractWorkflow {

    @Override
    public void configure() {
      setName("SleepWorkflow");
      setDescription("FunWorkflow description");
      addAction(new CustomAction("verify"));
    }
  }

  /**
   * CustomAction
   */
  public static final class CustomAction extends AbstractCustomAction {

    private static final Logger LOG = LoggerFactory.getLogger(CustomAction.class);

    private final String name;

    public CustomAction(String name) {
      this.name = name;
    }

    @Override
    public void configure() {
      setName(name);
      setDescription(name);
    }

    @Override
    public void initialize() {
      LOG.info("Custom action initialized: " + getContext().getSpecification().getName());
    }

    @Override
    public void destroy() {
      super.destroy();
      LOG.info("Custom action destroyed: " + getContext().getSpecification().getName());
    }

    @Override
    public void run() {
      LOG.info("Custom action run");
      try {
        String sleepTime = getContext().getRuntimeArguments().get("sleep.ms");
        Thread.sleep(sleepTime == null ? 2000 : Long.parseLong(sleepTime));
      } catch (InterruptedException e) {
        // expected if the workflow get killed
      }
      LOG.info("Custom run completed.");
    }
  }

  @Plugin(type = "simple")
  @Name("SimpleRunnablePlugin")
  @Requirements(capabilities = "healthcare")
  public class SimplePlugin {

    public long time() {
      return System.currentTimeMillis();
    }
  }
}
