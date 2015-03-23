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

package co.cask.cdap.test.app;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.worker.AbstractWorker;
import co.cask.cdap.api.worker.WorkerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Worker at App Level.
 */
public class AppWithWorker extends AbstractApplication {

  public static final String NAME = "AppWithWorker";
  public static final String WORKER = "TableWriter";
  public static final String DATASET = "MyKVTable";
  public static final String INITIALIZE = "initialize";
  public static final String RUN = "run";
  public static final String STOP = "stop";

  @Override
  public void configure() {
    setName(NAME);
    addWorker(new TableWriter());
    createDataset(DATASET, KeyValueTable.class);
  }

  private static class TableWriter extends AbstractWorker {
    private static final Logger LOG = LoggerFactory.getLogger(TableWriter.class);
    private volatile boolean running;

    @Override
    public void configure() {
      setInstances(3);
    }

    @Override
    public void initialize(WorkerContext context) throws Exception {
      super.initialize(context);
      writeToTable(INITIALIZE, INITIALIZE);
    }

    @Override
    public void run() {
      LOG.error("Running Worker Instance : {} : {}", getContext().getInstanceId(), getContext().getInstanceCount());
      running = true;
      writeToTable(RUN, RUN);
      while (running) {
        try {
          TimeUnit.MILLISECONDS.sleep(100);
        } catch (InterruptedException e) {

        }
      }
    }

    @Override
    public void onSuspend() {
      if (getContext().getInstanceId() == 0) {
        LOG.error("Starting suspension of Worker Instance : {} : {}",
                  getContext().getInstanceId(), getContext().getInstanceCount());
        try {
          TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
          LOG.error("Error while suspending Worker Instance : {} : {}",
                    getContext().getInstanceId(), getContext().getInstanceCount());
        }
      }
      LOG.error("Suspending Worker Instance : {} : {}", getContext().getInstanceId(), getContext().getInstanceCount());
    }

    @Override
    public void onResume() {
      LOG.error("Resuming Worker Instance : {} : {}", getContext().getInstanceId(), getContext().getInstanceCount());
    }

    @Override
    public void stop() {
      running = false;
      writeToTable(STOP, STOP);
    }

    private void writeToTable(final String key, final String value) {
//      getContext().execute(new TxRunnable() {
//        @Override
//        public void run(DatasetContext context) throws Exception {
//          KeyValueTable table = context.getDataset(DATASET);
//          table.write(key, value);
//        }
//      });
    }
  }
}
