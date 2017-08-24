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

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.security.store.SecureStoreData;
import co.cask.cdap.api.workflow.NodeValue;
import co.cask.cdap.api.workflow.WorkflowContext;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.etl.api.condition.ConditionContext;
import co.cask.cdap.etl.api.condition.StageStatistics;
import co.cask.cdap.etl.common.AbstractStageContext;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.PipelineRuntime;
import co.cask.cdap.etl.spec.StageSpec;
import com.google.common.collect.ImmutableMap;
import org.apache.tephra.TransactionFailureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Default implementation of the {@link ConditionContext}.
 */
public class BasicConditionContext extends AbstractStageContext implements ConditionContext {
  private static final Logger LOG = LoggerFactory.getLogger(BasicConditionContext.class);
  private final WorkflowContext context;
  private final Map<String, StageStatistics> stageStatistics;

  public BasicConditionContext(WorkflowContext context, PipelineRuntime pipelineRuntime, StageSpec stageSpec) {
    super(pipelineRuntime, stageSpec);
    this.context = context;
    this.stageStatistics = ImmutableMap.copyOf(createStageStatistics(context));
  }


  private Map<String, StageStatistics> createStageStatistics(WorkflowContext context) {
    Map<String, StageStatistics> stageStatistics = new HashMap<>();
    WorkflowToken token = context.getToken();
    for (WorkflowToken.Scope scope : Arrays.asList(WorkflowToken.Scope.SYSTEM, WorkflowToken.Scope.USER)) {
      Map<String, List<NodeValue>> all = token.getAll(scope);
      for (Map.Entry<String, List<NodeValue>> entry : all.entrySet()) {
        if (!entry.getKey().startsWith(Constants.StageStatistics.PREFIX + ".")) {
          continue;
        }
        String stageKey = entry.getKey().substring(Constants.StageStatistics.PREFIX.length() + 1);

        String stageName;
        if (stageKey.endsWith(Constants.StageStatistics.INPUT_RECORDS)) {
          stageName = stageKey.substring(0, stageKey.length() - Constants.StageStatistics.INPUT_RECORDS.length() - 1);
        } else if (stageKey.endsWith(Constants.StageStatistics.OUTPUT_RECORDS)) {
          stageName = stageKey.substring(0, stageKey.length() - Constants.StageStatistics.OUTPUT_RECORDS.length() - 1);
        } else if (stageKey.endsWith(Constants.StageStatistics.ERROR_RECORDS)) {
          stageName = stageKey.substring(0, stageKey.length() - Constants.StageStatistics.ERROR_RECORDS.length() - 1);
        } else {
          // should not happen
          LOG.warn(String.format("Ignoring key '%s' in the Workflow token while generating stage statistics " +
                                   "because it is not in the form " +
                                   "'stage.statistics.<stage_name>.<input|output|error>.records'.",
                                 stageKey));
          continue;
        }

        // Since stage names are unique and properties for each stage tracked are unique(input, output, and error)
        // there should only be single node who added this particular key in the Workflow
        long value = entry.getValue().get(0).getValue().getAsLong();

        StageStatistics statistics = stageStatistics.get(stageName);
        if (statistics == null) {
          statistics = new BasicStageStatistics(0, 0, 0);
          stageStatistics.put(stageName, statistics);
        }

        long numOfInputRecords = statistics.getInputRecordsCount();
        long numOfOutputRecords = statistics.getOutputRecordsCount();
        long numOfErrorRecords = statistics.getErrorRecordsCount();

        if (stageKey.endsWith(Constants.StageStatistics.INPUT_RECORDS)) {
          numOfInputRecords = value;
        } else if (stageKey.endsWith(Constants.StageStatistics.OUTPUT_RECORDS)) {
          numOfOutputRecords = value;
        } else {
          numOfErrorRecords = value;
        }
        stageStatistics.put(stageName, new BasicStageStatistics(numOfInputRecords, numOfOutputRecords,
                                                                numOfErrorRecords));
      }
    }
    return stageStatistics;
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
    return stageStatistics;
  }
}
