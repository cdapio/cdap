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

package co.cask.cdap.etl.common;

import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.etl.api.LookupProvider;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Map;

/**
 * A {@link ExecutionTransformContext} for that is available to plugins in the initialize() method.
 * Plugins can access resources localized for a pipeline using this context.
 */
public class ExecutionTransformContext extends AbstractTransformContext {

  private final Map<String, File> localizedResources;

  public ExecutionTransformContext(PluginContext pluginContext, Metrics metrics, LookupProvider lookup,
                                   String stageId, Map<String, File> localizedResources) {
    super(pluginContext, metrics, lookup, stageId);
    this.localizedResources = localizedResources;
  }

  @Override
  public File getLocalFile(String name) throws FileNotFoundException {
    if (!localizedResources.containsKey(name)) {
      throw new FileNotFoundException(String.format("Resource %s was not localized for this pipeline", name));
    }
    return localizedResources.get(name);
  }

  @Override
  public Map<String, File> getAllLocalFiles() {
    return localizedResources;
  }
}
