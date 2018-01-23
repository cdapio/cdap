/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.api.spark.service;

import co.cask.cdap.api.plugin.PluginConfigurer;
import co.cask.cdap.api.plugin.PluginContext;

import java.io.Closeable;

/**
 * A {@link PluginContext} for Spark HTTP service. It also implements {@link PluginConfigurer} to allow dynamically
 * adding plugins that are usable for Spark jobs submitted through the handler
 * methods in {@link SparkHttpServiceHandler}.
 */
public interface SparkHttpServicePluginContext extends PluginContext, PluginConfigurer, Closeable {

  /**
   * Close and release resources owned by this object.
   */
  @Override
  void close();
}
