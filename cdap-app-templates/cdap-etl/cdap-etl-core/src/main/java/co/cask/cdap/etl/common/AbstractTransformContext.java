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
import co.cask.cdap.etl.api.Lookup;
import co.cask.cdap.etl.api.LookupProvider;
import co.cask.cdap.etl.api.TransformContext;
import co.cask.cdap.etl.planner.StageInfo;

import java.util.Map;

/**
 * Base implementation of {@link TransformContext} for common functionality.
 * This context scopes plugin ids by the id of the stage. This allows multiple transforms to use plugins with
 * the same id without clobbering each other.
 */
public abstract class AbstractTransformContext extends AbstractStageContext implements TransformContext {

  private final LookupProvider lookup;

  public AbstractTransformContext(PluginContext pluginContext, Metrics metrics,
                                  LookupProvider lookup, StageInfo stageInfo, BasicArguments arguments) {
    super(pluginContext, metrics, stageInfo, arguments);
    this.lookup = lookup;
  }

  @Override
  public <T> Lookup<T> provide(String table, Map<String, String> arguments) {
    return lookup.provide(table, arguments);
  }
}
