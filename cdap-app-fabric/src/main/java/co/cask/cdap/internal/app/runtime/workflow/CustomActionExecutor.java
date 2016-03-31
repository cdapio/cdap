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

package co.cask.cdap.internal.app.runtime.workflow;

import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.workflow.WorkflowAction;
import co.cask.cdap.app.metrics.ProgramUserMetrics;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.CombineClassLoader;
import co.cask.cdap.common.lang.InstantiatorFactory;
import co.cask.cdap.common.lang.PropertyFieldSetter;
import co.cask.cdap.internal.app.runtime.DataSetFieldSetter;
import co.cask.cdap.internal.app.runtime.MetricsFieldSetter;
import co.cask.cdap.internal.lang.Reflections;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionFailureException;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Execute the custom action in the Workflow.
 */
public class CustomActionExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(CustomActionExecutor.class);
  private final ProgramRunId workflowRunId;
  private final BasicWorkflowContext workflowContext;
  private final WorkflowAction action;

  /**
   * Creates instance which will be used to initialize, run, and destroy the custom action.
   * @param workflowRunId the Workflow run which started the execution of this custom action.
   * @param workflowContext an instance of context
   * @param instantiator to instantiates the custom action class
   * @param classLoader used to load the custom action class
   * @throws Exception when failed to instantiate the custom action
   */
  public CustomActionExecutor(ProgramRunId workflowRunId, BasicWorkflowContext workflowContext,
                              InstantiatorFactory instantiator, ClassLoader classLoader) throws Exception {
    this.workflowRunId = workflowRunId;
    this.workflowContext = workflowContext;
    this.action = createAction(workflowContext, instantiator, classLoader);
  }

  @SuppressWarnings("unchecked")
  private WorkflowAction createAction(BasicWorkflowContext context, InstantiatorFactory instantiator,
                                      ClassLoader classLoader) throws Exception {
    Class<?> clz = Class.forName(context.getSpecification().getClassName(), true, classLoader);
    Preconditions.checkArgument(WorkflowAction.class.isAssignableFrom(clz), "%s is not a WorkflowAction.", clz);
    WorkflowAction action = instantiator.get(TypeToken.of((Class<? extends WorkflowAction>) clz)).create();
    Metrics metrics = new ProgramUserMetrics(
      context.getProgramMetrics().childContext(Constants.Metrics.Tag.NODE, context.getSpecification().getName()));
    Reflections.visit(action, action.getClass(),
                      new PropertyFieldSetter(context.getSpecification().getProperties()),
                      new DataSetFieldSetter(context),
                      new MetricsFieldSetter(metrics));
    return action;
  }

  void execute() throws Exception {
    ClassLoader oldClassLoader = setContextCombinedClassLoader(action);
    try {
      initializeInTransaction();
      runInTransaction();
      workflowContext.setSuccess();
    } finally {
      destroyInTransaction();
      ClassLoaders.setContextClassLoader(oldClassLoader);
    }
  }

  private void initializeInTransaction() throws Exception {
    TransactionContext txContext = workflowContext.getDatasetCache().newTransactionContext();
    txContext.start();
    try {
      action.initialize(workflowContext);
      txContext.finish();
    } catch (TransactionFailureException e) {
      txContext.abort(e);
    } catch (Throwable t) {
      txContext.abort(new TransactionFailureException("Transaction function failure for transaction. ", t));
    }
  }

  private void runInTransaction() throws Exception {
    TransactionContext txContext = workflowContext.getDatasetCache().newTransactionContext();
    txContext.start();
    try {
      action.run();
      txContext.finish();
    } catch (TransactionFailureException e) {
      txContext.abort(e);
    } catch (Throwable t) {
      txContext.abort(new TransactionFailureException("Transaction function failure for transaction. ", t));
    }
  }

  private void destroyInTransaction() {
    TransactionContext txContext = workflowContext.getDatasetCache().newTransactionContext();
    try {
      txContext.start();
      try {
        action.destroy();
        txContext.finish();
      } catch (TransactionFailureException e) {
        txContext.abort(e);
      } catch (Throwable t) {
        txContext.abort(new TransactionFailureException("Transaction function failure for transaction. ", t));
      }
    } catch (Throwable t) {
      LOG.error("Failed to execute the destroy method on action {} for Workflow run {}",
                workflowContext.getSpecification().getName(), workflowRunId, t);
    }
  }

  private ClassLoader setContextCombinedClassLoader(WorkflowAction action) {
    return ClassLoaders.setContextClassLoader(
      new CombineClassLoader(null, ImmutableList.of(action.getClass().getClassLoader(), getClass().getClassLoader())));
  }
}
