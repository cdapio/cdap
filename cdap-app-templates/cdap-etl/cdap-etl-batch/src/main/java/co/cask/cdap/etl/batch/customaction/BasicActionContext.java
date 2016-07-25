/*
 * Copyright © 2016 Cask Data, Inc.
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
package co.cask.cdap.etl.batch.customaction;

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.customaction.CustomActionContext;
import co.cask.cdap.api.macro.InvalidMacroException;
import co.cask.cdap.api.macro.MacroEvaluator;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.api.security.store.SecureStoreData;
import co.cask.cdap.api.security.store.SecureStoreMetadata;
import co.cask.cdap.etl.api.action.ActionContext;
import co.cask.cdap.etl.api.action.SettableArguments;
import co.cask.tephra.TransactionFailureException;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Default implementation for the {@link ActionContext}.
 */
public class BasicActionContext implements ActionContext {

  private final CustomActionContext context;
  private final BasicSettableArguments arguments;

  public BasicActionContext(CustomActionContext context) {
    this.context = context;
    this.arguments = new BasicSettableArguments(context.getRuntimeArguments());
  }

  @Override
  public long getLogicalStartTime() {
    return context.getLogicalStartTime();
  }

  @Override
  public SettableArguments getArguments() {
    return arguments;
  }

  @Override
  public PluginProperties getPluginProperties(String pluginId) {
    return context.getPluginProperties(pluginId);
  }

  @Override
  public <T> Class<T> loadPluginClass(String pluginId) {
    return context.loadPluginClass(pluginId);
  }

  @Override
  public <T> T newPluginInstance(String pluginId) throws InstantiationException {
    return context.newPluginInstance(pluginId);
  }

  @Override
  public <T> T newPluginInstance(String pluginId, MacroEvaluator evaluator)
    throws InstantiationException, InvalidMacroException {
    return context.newPluginInstance(pluginId, evaluator);
  }

  @Override
  public void execute(TxRunnable runnable) throws TransactionFailureException {
    context.execute(runnable);
  }

  @Override
  public List<SecureStoreMetadata> listSecureData(String namespace) throws IOException {
    return context.listSecureData(namespace);
  }

  @Override
  public SecureStoreData getSecureData(String namespace, String name) throws IOException {
    return context.getSecureData(namespace, name);
  }

  @Override
  public void putSecureData(String namespace, String name, byte[] data, String description,
                            Map<String, String> properties) throws IOException {
    context.getAdmin().putSecureData(namespace, name, data, description, properties);
  }

  @Override
  public void deleteSecureData(String namespace, String name) throws IOException {
    context.getAdmin().deleteSecureData(namespace, name);
  }

  @Override
  public String getNamespace() {
    return context.getNamespace();
  }
}
