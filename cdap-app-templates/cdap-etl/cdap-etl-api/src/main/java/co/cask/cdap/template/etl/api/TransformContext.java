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

package co.cask.cdap.template.etl.api;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.templates.plugins.PluginProperties;

/**
 * Context passed to ETL stages.
 */
@Beta
public interface TransformContext {

  /**
   * Gets the {@link PluginProperties} associated with the stage.
   *
   * @return the {@link PluginProperties}.
   */
  PluginProperties getPluginProperties();

  /**
   * Get an instance of {@link Metrics}, used to collect metrics. Note that metric names are not scoped by
   * the stage they are emitted from. A metric called 'reads' emitted in one stage will be aggregated with
   * those emitted in another stage.
   *
   * @return {@link Metrics} for collecting metrics
   */
  Metrics getMetrics();
}
