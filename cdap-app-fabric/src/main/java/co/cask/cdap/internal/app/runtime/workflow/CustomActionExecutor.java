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

import co.cask.cdap.api.ProgramState;
import co.cask.cdap.api.ProgramStatus;
import co.cask.cdap.api.annotation.TransactionControl;
import co.cask.cdap.api.customaction.AbstractCustomAction;
import co.cask.cdap.api.customaction.CustomAction;
import co.cask.cdap.api.customaction.CustomActionContext;
import co.cask.cdap.common.lang.InstantiatorFactory;
import co.cask.cdap.common.lang.PropertyFieldSetter;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.internal.app.runtime.AbstractContext;
import co.cask.cdap.internal.app.runtime.DataSetFieldSetter;
import co.cask.cdap.internal.app.runtime.MetricsFieldSetter;
import co.cask.cdap.internal.app.runtime.customaction.BasicCustomActionContext;
import co.cask.cdap.internal.lang.Reflections;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Execute the custom action in the Workflow.
 */
class CustomActionExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(CustomActionExecutor.class);
  private final CustomAction customAction;
  private final BasicCustomActionContext customActionContext;

  /**
   * Creates instance which will be used to initialize, run, and destroy the custom action.
   * @param customActionContext an instance of context
   * @param instantiator to instantiates the custom action class
   * @param classLoader used to load the custom action class
   * @throws Exception when failed to instantiate the custom action
   */
  CustomActionExecutor(BasicCustomActionContext customActionContext,
                       InstantiatorFactory instantiator, ClassLoader classLoader) throws Exception {
    this.customActionContext = customActionContext;
    this.customAction = createCustomAction(customActionContext, instantiator, classLoader);
  }

  @SuppressWarnings("unchecked")
  private CustomAction createCustomAction(BasicCustomActionContext context, InstantiatorFactory instantiator,
                                          ClassLoader classLoader) throws Exception {
    Class<?> clz = Class.forName(context.getSpecification().getClassName(), true, classLoader);
    Preconditions.checkArgument(CustomAction.class.isAssignableFrom(clz), "%s is not a CustomAction.", clz);
    CustomAction action = instantiator.get(TypeToken.of((Class<? extends CustomAction>) clz)).create();
    Reflections.visit(action, action.getClass(),
                      new PropertyFieldSetter(context.getSpecification().getProperties()),
                      new DataSetFieldSetter(context),
                      new MetricsFieldSetter(context.getMetrics()));
    return action;
  }

  void execute() throws Exception {
    try {
      customActionContext.setState(new ProgramState(ProgramStatus.INITIALIZING, null));
      // AbstractCustomAction implements final initialize(context) and requires subclass to
      // implement initialize(), whereas programs that directly implement CustomAction can
      // override initialize(context)
      TransactionControl txControl = customAction instanceof AbstractCustomAction
        ? Transactions.getTransactionControl(TransactionControl.IMPLICIT, AbstractCustomAction.class,
                                             customAction, "initialize")
        : Transactions.getTransactionControl(TransactionControl.IMPLICIT, CustomAction.class,
                                             customAction, "initialize", CustomActionContext.class);
      customActionContext.initializeProgram(customAction, txControl, false);

      customActionContext.setState(new ProgramState(ProgramStatus.RUNNING, null));
      customActionContext.executeChecked(new AbstractContext.ThrowingRunnable() {
        @Override
        public void run() throws Exception {
          customAction.run();
        }
      });
      customActionContext.setState(new ProgramState(ProgramStatus.COMPLETED, null));

    } catch (Throwable t) {
      customActionContext.setState(new ProgramState(ProgramStatus.FAILED, Throwables.getRootCause(t).getMessage()));
      Throwables.propagateIfPossible(t, Exception.class);
      throw Throwables.propagate(t);

    } finally {
      TransactionControl txControl = Transactions.getTransactionControl(TransactionControl.IMPLICIT,
                                                                        CustomAction.class, customAction, "destroy");
      customActionContext.destroyProgram(customAction, txControl, false);
    }
  }
}
