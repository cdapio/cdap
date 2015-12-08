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

package co.cask.cdap.etl.batch;

import co.cask.cdap.etl.api.batch.BatchConfigurable;
import co.cask.cdap.etl.api.batch.BatchContext;
import co.cask.cdap.etl.log.LogContext;

import java.util.concurrent.Callable;

/**
 * Wrapper around a BatchConfigurable that makes sure logging is set up correctly.
 *
 * @param <T> type of context
 */
public class LoggedBatchConfigurable<T extends BatchContext> extends BatchConfigurable<T> {
  private final BatchConfigurable<T> batchConfigurable;
  private final String name;

  public LoggedBatchConfigurable(String name, BatchConfigurable<T> batchConfigurable) {
    this.name = name;
    this.batchConfigurable = batchConfigurable;
  }

  @Override
  public void prepareRun(final T context) throws Exception {
    LogContext.run(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        batchConfigurable.prepareRun(context);
        return null;
      }
    }, name);
  }

  @Override
  public void onRunFinish(final boolean succeeded, final T context) {
    LogContext.runUnchecked(new Callable<Void>() {
      @Override
      public Void call() {
        batchConfigurable.onRunFinish(succeeded, context);
        return null;
      }
    }, name);
  }
}
