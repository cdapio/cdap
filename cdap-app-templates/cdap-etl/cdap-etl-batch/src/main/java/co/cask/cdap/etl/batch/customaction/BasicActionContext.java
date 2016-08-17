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
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.api.security.store.SecureStoreData;
import co.cask.cdap.api.security.store.SecureStoreMetadata;
import co.cask.cdap.etl.api.StageContext;
import co.cask.cdap.etl.api.action.ActionContext;
import co.cask.cdap.etl.api.action.SettableArguments;
import co.cask.cdap.etl.common.AbstractStageContext;
import co.cask.tephra.TransactionFailureException;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Default implementation for the {@link ActionContext}.
 */
public class BasicActionContext extends AbstractStageContext implements ActionContext  {

  private final CustomActionContext context;
  private final BasicSettableArguments arguments;

  public BasicActionContext(CustomActionContext context, Metrics metrics, String stageName) {
    super(context, metrics, stageName);
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
  public void execute(TxRunnable runnable) throws TransactionFailureException {
    context.execute(runnable);
  }

  @Override
  public List<SecureStoreMetadata> listSecureData(String namespace) throws Exception {
    return context.listSecureData(namespace);
  }

  @Override
  public SecureStoreData getSecureData(String namespace, String name) throws Exception {
    return context.getSecureData(namespace, name);
  }

  @Override
  public void putSecureData(String namespace, String name, String data, String description,
                            Map<String, String> properties) throws Exception {
    context.getAdmin().putSecureData(namespace, name, data, description, properties);
  }

  @Override
  public void deleteSecureData(String namespace, String name) throws Exception {
    context.getAdmin().deleteSecureData(namespace, name);
  }

  @Override
  public String getNamespace() {
    return context.getNamespace();
  }

}
