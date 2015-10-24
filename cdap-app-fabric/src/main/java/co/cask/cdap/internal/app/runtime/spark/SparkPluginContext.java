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

package co.cask.cdap.internal.app.runtime.spark;

import co.cask.cdap.api.plugin.Plugin;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.internal.app.runtime.DefaultPluginContext;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.proto.Id;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link Externalizable} implementation of {@link PluginContext} used in Spark program execution.
 * It has no-op for serialize/deserialize operations. It uses {@link SparkContextProvider} to
 * get the {@link ExecutionSparkContext} in the current execution context.
 */
public class SparkPluginContext implements PluginContext, Externalizable {

  private final PluginContext delegate;

  public SparkPluginContext() {
    this(SparkContextProvider.getSparkContext());
  }

  public SparkPluginContext(ExecutionSparkContext context) {
    this(context.getPluginInstantiator(), context.getProgramId(), context.getApplicationSpecification().getPlugins());
  }

  public SparkPluginContext(@Nullable PluginInstantiator pluginInstantiator,
                            Id.Program programId, Map<String, Plugin> plugins) {
    this.delegate = new DefaultPluginContext(pluginInstantiator, programId, plugins);
  }

  @Override
  public PluginProperties getPluginProperties(String pluginId) {
    return delegate.getPluginProperties(pluginId);
  }

  @Override
  public <T> Class<T> loadPluginClass(String pluginId) {
    return delegate.loadPluginClass(pluginId);
  }

  @Override
  public <T> T newPluginInstance(String pluginId) throws InstantiationException {
    return delegate.newPluginInstance(pluginId);
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    // no-op
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    // no-op
  }
}
