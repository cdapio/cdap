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

package co.cask.cdap.etl.common.submit;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.etl.api.StageContext;
import co.cask.cdap.etl.api.SubmitterLifecycle;
import org.apache.tephra.TransactionFailureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * Runs SubmitterLifecycle methods within a transaction.
 *
 * @param <T> type of context for the SubmitterLifecycle
 * @param <U> type of context provided by the context provider
 */
public class SubmitterPlugin<T, U extends T> implements Preparer, Finisher {
  private static final Logger LOG = LoggerFactory.getLogger(SubmitterPlugin.class);
  private final String stageName;
  private final Transactional transactional;
  private final SubmitterLifecycle<T> delegate;
  private final ContextProvider<U> contextProvider;
  private final PrepareAction<U> prepareAction;
  private StageContext stageContext;

  public SubmitterPlugin(String stageName, Transactional transactional,
                         SubmitterLifecycle<T> delegate,
                         ContextProvider<U> contextProvider) {
    this(stageName, transactional, delegate, contextProvider, new PrepareAction<U>() {
      @Override
      public void act(U context) {
        // no-op
      }
    });
  }
  public SubmitterPlugin(String stageName, Transactional transactional,
                         SubmitterLifecycle<T> delegate,
                         ContextProvider<U> contextProvider,
                         PrepareAction<U> prepareAction) {
    this.stageName = stageName;
    this.transactional = transactional;
    this.delegate = delegate;
    this.contextProvider = contextProvider;
    this.prepareAction = prepareAction;
  }

  @Override
  public void onFinish(final boolean succeeded) {
    try {
      transactional.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext datasetContext) throws Exception {
          T context = contextProvider.getContext(datasetContext);
          delegate.onRunFinish(succeeded, context);
        }
      });
    } catch (TransactionFailureException e) {
      LOG.warn("Error calling onRunFinish on stage {}", stageName);
    }
  }

  @Override
  public void prepareRun() throws TransactionFailureException {
    transactional.execute(new TxRunnable() {
      @Override
      public void run(DatasetContext datasetContext) throws Exception {
        U context = contextProvider.getContext(datasetContext);
        delegate.prepareRun(context);
        if (context instanceof StageContext) {
          stageContext = (StageContext) context;
        }
        prepareAction.act(context);
      }
    });
  }

  @Nullable
  public StageContext getStageContext() {
    return stageContext;
  }

  /**
   * Some additional action to run during preparation
   *
   * @param <T> type of context
   */
  public interface PrepareAction<T> {

    void act(T context);
  }
}
