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

package co.cask.cdap.internal.app.runtime.worker;

import co.cask.cdap.api.annotation.TransactionControl;
import co.cask.cdap.api.worker.Worker;
import co.cask.cdap.api.worker.WorkerContext;
import co.cask.cdap.api.worker.WorkerSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.common.lang.InstantiatorFactory;
import co.cask.cdap.common.lang.PropertyFieldSetter;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.internal.app.runtime.AbstractContext;
import co.cask.cdap.internal.app.runtime.MetricsFieldSetter;
import co.cask.cdap.internal.lang.Reflections;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;

/**
 * A {@link Service} for executing {@link Worker}s.
 */
public class WorkerDriver extends AbstractExecutionThreadService {
  private static final Logger LOG = LoggerFactory.getLogger(WorkerDriver.class);

  private final Program program;
  private final WorkerSpecification spec;
  private final BasicWorkerContext context;

  private Worker worker;

  public WorkerDriver(Program program, WorkerSpecification spec, BasicWorkerContext context) {
    this.program = program;
    this.spec = spec;
    this.context = context;
  }

  @Override
  protected void startUp() throws Exception {
    LoggingContextAccessor.setLoggingContext(context.getLoggingContext());

    // Instantiate worker instance
    Class<?> workerClass = program.getClassLoader().loadClass(spec.getClassName());
    @SuppressWarnings("unchecked")
    TypeToken<Worker> workerType = (TypeToken<Worker>) TypeToken.of(workerClass);
    worker = new InstantiatorFactory(false).get(workerType).create();

    // Fields injection
    Reflections.visit(worker, workerType.getType(),
                      new MetricsFieldSetter(context.getMetrics()),
                      new PropertyFieldSetter(spec.getProperties()));

    LOG.debug("Starting Worker Program {}", program.getId());

    // Initialize worker
    TransactionControl txControl = Transactions.getTransactionControl(TransactionControl.EXPLICIT, Worker.class,
                                                                      worker, "initialize", WorkerContext.class);
    context.initializeProgram(worker, txControl, false);
  }

  @Override
  protected void run() throws Exception {
    context.executeChecked(worker::run);
  }

  @Override
  protected void shutDown() throws Exception {
    if (worker == null) {
      return;
    }
    TransactionControl txControl = Transactions.getTransactionControl(TransactionControl.EXPLICIT,
                                                                      Worker.class, worker, "destroy");
    context.destroyProgram(worker, txControl, false);
    context.close();
  }

  @Override
  protected void triggerShutdown() {
    if (worker == null) {
      return;
    }
    LOG.debug("Stopping Worker Program {}", program.getId());
    context.executeUnchecked(new Runnable() {
      @Override
      public void run() {
        worker.stop();
      }
    });
  }

  @Override
  protected Executor executor() {
    return new Executor() {
      @Override
      public void execute(Runnable command) {
        Thread t = new Thread(command, String.format("worker-%s-%d", program.getName(), context.getInstanceId()));
        t.setDaemon(true);
        t.start();
      }
    };
  }

  public void setInstanceCount(int instanceCount) {
    context.setInstanceCount(instanceCount);
  }
}
