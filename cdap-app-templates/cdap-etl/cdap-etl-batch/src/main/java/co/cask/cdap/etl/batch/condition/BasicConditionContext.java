/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.etl.batch.condition;

import co.cask.cdap.api.ServiceDiscoverer;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.security.store.SecureStoreData;
import co.cask.cdap.api.workflow.WorkflowContext;
import co.cask.cdap.etl.api.condition.ConditionContext;
import co.cask.cdap.etl.api.condition.StageStatistics;
import co.cask.cdap.etl.common.AbstractStageContext;
import co.cask.cdap.etl.common.BasicArguments;
import co.cask.cdap.etl.common.PipelineRuntime;
import co.cask.cdap.etl.spec.StageSpec;
import org.apache.tephra.TransactionFailureException;

import java.util.HashMap;
import java.util.Map;

/**
 * Default implementation of the {@link ConditionContext}.
 */
public class BasicConditionContext extends AbstractStageContext implements ConditionContext {

  private final WorkflowContext context;

  public BasicConditionContext(WorkflowContext context, ServiceDiscoverer serviceDiscoverer, Metrics metrics,
                               StageSpec stageInfo, BasicArguments arguments) {
    super(new PipelineRuntime(context, metrics), stageInfo);
    this.context = context;
  }

  @Override
  public Map<String, String> listSecureData(String namespace) throws Exception {
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
  public void execute(TxRunnable runnable) throws TransactionFailureException {
    context.execute(runnable);
  }

  @Override
  public void execute(int timeoutInSeconds, TxRunnable runnable) throws TransactionFailureException {
    context.execute(timeoutInSeconds, runnable);
  }

  @Override
  public Map<String, StageStatistics> getStageStatistics() {
    return new HashMap<>();
  }
}
