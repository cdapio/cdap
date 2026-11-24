/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.client.app;

import io.cdap.cdap.api.TxRunnable;
import io.cdap.cdap.api.app.ApplicationConfigurer;
import io.cdap.cdap.api.app.ApplicationContext;
import io.cdap.cdap.api.worker.Worker;
import io.cdap.cdap.api.worker.WorkerContext;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.tephra.TransactionFailureException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link ConfigurableProgramsApp}.
 */
public class ConfigurableProgramsAppTest {

  @Test
  @SuppressWarnings("unchecked")
  public void testWorkerRunPropagatesTransactionFailureException() throws Exception {
    ConfigurableProgramsApp.Programs programs =
        new ConfigurableProgramsApp.Programs("myWorker", null, "myDataset");
    AtomicReference<Worker> capturedWorker = new AtomicReference<>();

    ApplicationConfigurer configurer = (ApplicationConfigurer) Proxy.newProxyInstance(
        ApplicationConfigurer.class.getClassLoader(),
        new Class<?>[]{ApplicationConfigurer.class},
        (proxy, method, args) -> {
          if ("addWorker".equals(method.getName())) {
            capturedWorker.set((Worker) args[0]);
          }
          return null;
        });

    ApplicationContext<ConfigurableProgramsApp.Programs> appContext =
        (ApplicationContext<ConfigurableProgramsApp.Programs>) Proxy.newProxyInstance(
            ApplicationContext.class.getClassLoader(),
            new Class<?>[]{ApplicationContext.class},
            (proxy, method, args) -> "getConfig".equals(method.getName()) ? programs : null);

    ConfigurableProgramsApp app = new ConfigurableProgramsApp();
    app.configure(configurer, appContext);
    Worker worker = capturedWorker.get();
    Assert.assertNotNull(worker);

    AtomicBoolean executeCalled = new AtomicBoolean(false);
    WorkerContext okContext = createWorkerContext(runnable -> executeCalled.set(true));
    worker.initialize(okContext);
    worker.run();
    Assert.assertTrue(executeCalled.get());

    TransactionFailureException txFailure = new TransactionFailureException("tx failed");
    WorkerContext failingContext = createWorkerContext(runnable -> {
      throw txFailure;
    });
    worker.initialize(failingContext);
    try {
      worker.run();
      Assert.fail("Expected RuntimeException wrapping TransactionFailureException");
    } catch (RuntimeException actual) {
      Assert.assertSame(txFailure, actual.getCause());
    }
  }

  private interface ExecuteHandler {
    void execute(TxRunnable runnable) throws TransactionFailureException;
  }

  private WorkerContext createWorkerContext(ExecuteHandler handler) {
    return (WorkerContext) Proxy.newProxyInstance(
        WorkerContext.class.getClassLoader(),
        new Class<?>[]{WorkerContext.class},
        (proxy, method, args) -> {
          if ("execute".equals(method.getName()) && args != null && args.length == 1) {
            handler.execute((TxRunnable) args[0]);
          }
          return null;
        });
  }
}
