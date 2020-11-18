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

package io.cdap.cdap.internal.app.runtime.artifact.app.inspection;

import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.annotation.Requirements;
import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.api.plugin.PluginConfig;

/**
 * App used in artifact inspector tests
 */
@Requirements(capabilities = "cdc")
public class InspectionApp extends AbstractApplication<InspectionApp.AConfig> {
  public static final String PLUGIN_DESCRIPTION = "some plugin";
  public static final String PLUGIN_NAME = "pluginA";
  public static final String PLUGIN_TYPE = "A";
  public static final String MULTIPLE_REQUIREMENTS_PLUGIN = "MultipleRequirementsPlugin";

  public static class AConfig extends Config {
    private int x;
    private String str;
  }

  public static class PConfig extends PluginConfig {
    @Macro
    private double y;
    private boolean isSomething;
  }

  @Override
  public void configure() {
    // nothing since its not a real app
  }

  @Plugin(type = PLUGIN_TYPE)
  @Name(PLUGIN_NAME)
  @Description(PLUGIN_DESCRIPTION)
  public static class AppPlugin {
    private PConfig pluginConf;

    public double doSomething() {
      return pluginConf.y;
    }
  }

  @Plugin(type = PLUGIN_TYPE)
  @Name("SingleRequirementPlugin")
  @Description(PLUGIN_DESCRIPTION)
  @Requirements(datasetTypes = Table.TYPE)
  public static class SingleRequirementPlugin {
    private PConfig pluginConf;

    public double doSomething() {
      return pluginConf.y;
    }
  }

  @Plugin(type = PLUGIN_TYPE)
  @Name(MULTIPLE_REQUIREMENTS_PLUGIN)
  @Description(PLUGIN_DESCRIPTION)
  @Requirements(datasetTypes = {Table.TYPE, KeyValueTable.TYPE})
  public static class MultipleRequirementsPlugin {
    private PConfig pluginConf;

    public double doSomething() {
      return pluginConf.y;
    }
  }

  @Plugin(type = PLUGIN_TYPE)
  @Name("EmptyRequirementPlugin")
  @Description(PLUGIN_DESCRIPTION)
  @Requirements(datasetTypes = {})
  public static class EmptyRequirementPlugin {
    private PConfig pluginConf;

    public double doSomething() {
      return pluginConf.y;
    }
  }

  @Plugin(type = PLUGIN_TYPE)
  @Name("SingleEmptyRequirementPlugin")
  @Description(PLUGIN_DESCRIPTION)
  @Requirements(datasetTypes = "")
  public static class SingleEmptyRequirementPlugin {
    private PConfig pluginConf;

    public double doSomething() {
      return pluginConf.y;
    }
  }

  @Plugin(type = PLUGIN_TYPE)
  @Name("ValidAndEmptyRequirementsPlugin")
  @Description(PLUGIN_DESCRIPTION)
  @Requirements(datasetTypes = {Table.TYPE, ""})
  public static class ValidAndEmptyRequirementsPlugin {
    private PConfig pluginConf;

    public double doSomething() {
      return pluginConf.y;
    }
  }

  @Plugin(type = PLUGIN_TYPE)
  @Name("DuplicateRequirementsPlugin")
  @Description(PLUGIN_DESCRIPTION)
  @Requirements(datasetTypes = {Table.TYPE, "   DupliCate    ", "    duplicate    "})
  public static class DuplicateRequirementsPlugin {
    private PConfig pluginConf;

    public double doSomething() {
      return pluginConf.y;
    }
  }

  @Plugin(type = PLUGIN_TYPE)
  @Name("NonTransactionalPlugin")
  @Description(PLUGIN_DESCRIPTION)
  @Requirements(datasetTypes = {"req1", "req2"})
  public static class NonTransactionalPlugin {
    private PConfig pluginConf;

    public double doSomething() {
      return pluginConf.y;
    }
  }

  @Plugin(type = PLUGIN_TYPE)
  @Name("Capability" +
    "Plugin")
  @Description(PLUGIN_DESCRIPTION)
  @Requirements(capabilities = {"cdc"})
  public static class CapabilityPlugin {
    private PConfig pluginConf;

    public double doSomething() {
      return pluginConf.y;
    }
  }

  @Plugin(type = PLUGIN_TYPE)
  @Name("MultipleCapabilityPlugin")
  @Description(PLUGIN_DESCRIPTION)
  @Requirements(capabilities = {"cdc", "healthcare"})
  public static class MultipleCapabilityPlugin {
    private PConfig pluginConf;

    public double doSomething() {
      return pluginConf.y;
    }
  }

  @Plugin(type = PLUGIN_TYPE)
  @Name("DatasetAndCapabilityPlugin")
  @Description(PLUGIN_DESCRIPTION)
  @Requirements(datasetTypes = {Table.TYPE, "sometype"}, capabilities = {"cdc", "healthcare"})
  public static class DatasetAndCapabilityPlugin {
    private PConfig pluginConf;

    public double doSomething() {
      return pluginConf.y;
    }
  }
}
