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

package io.cdap.cdap;

import com.google.common.base.Throwables;
import io.cdap.cdap.api.TxRunnable;
import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.worker.AbstractWorker;
import io.cdap.cdap.api.worker.WorkerContext;
import org.apache.tephra.TransactionFailureException;

import java.util.concurrent.TimeUnit;

/**
 * Worker at App Level.
 */
public class AppWithWorker extends AbstractApplication {

  public static final String NAME = "AppWithWorker";
  public static final String DESCRIPTION = "Application with Worker for Tests";
  public static final String WORKER = "TableWriter";
  public static final String DATASET = "MyKVTable";
  public static final String INITIALIZE = "initialize";
  public static final String RUN = "run";
  public static final String STOP = "stop";

  @Override
  public void configure() {
    setName(NAME);
    setDescription(DESCRIPTION);
    addWorker(new TableWriter());
  }

  public class TableWriter extends AbstractWorker {

    private volatile boolean stopped;

    @Override
    public void configure() {
      setName(WORKER);
      setDescription(DESCRIPTION);
      createDataset(DATASET, KeyValueTable.class);
    }

    @Override
    public void initialize(WorkerContext context) throws Exception {
      super.initialize(context);
      writeToTable(INITIALIZE, INITIALIZE);
    }

    @Override
    public void run() {
      writeToTable(RUN, RUN);
      while (!stopped) {
        try {
          TimeUnit.MILLISECONDS.sleep(100);
        } catch (InterruptedException e) {
          // ignore
        }
      }
    }

    @Override
    public void destroy() {
      writeToTable(STOP, STOP);
    }

    @Override
    public void stop() {
      stopped = true;
    }

    private void writeToTable(final String key, final String value) {
      try {
        getContext().execute(new TxRunnable() {
          @Override
          public void run(DatasetContext context) throws Exception {
            KeyValueTable table = context.getDataset(DATASET);
            table.write(key, value);
          }
        });
      } catch (TransactionFailureException e) {
        throw Throwables.propagate(e);
      }
    }
  }
}
