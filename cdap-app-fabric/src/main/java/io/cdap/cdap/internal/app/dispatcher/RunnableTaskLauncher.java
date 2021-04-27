/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.dispatcher;

import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.app.dispatcher.security.RunnableTaskPolicy;

import java.security.Policy;

/**
 * RunnableTaskLauncher launches a runnable task.
 */
public class RunnableTaskLauncher {
  private final CConfiguration cConfig;

  public RunnableTaskLauncher(CConfiguration cConfig) {
    this.cConfig = cConfig;
  }

  public byte[] launchRunnableTask(RunnableTaskRequest request) throws Exception {
    setSecurityManager();
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    Class<?> clazz = classLoader.loadClass(request.className);

    Injector injector = Guice.createInjector(new RunnableTaskModule(cConfig));
    Object obj = injector.getInstance(clazz);

    if (!(obj instanceof RunnableTask)) {
      throw new ClassCastException(String.format("%s is not a RunnableTask", request.className));
    }
    RunnableTask runnableTask = (RunnableTask) obj;
    if (runnableTask.start().get() != Service.State.RUNNING) {
      throw new Exception(String.format("service %s failed to start", request.className));
    }
    return runnableTask.runTask(request.param);
  }

  private void setSecurityManager() {
    SecurityManager sm = new SecurityManager();
    if (System.getSecurityManager() != null) {
      return;
    }
    Policy.setPolicy(new RunnableTaskPolicy());
    System.setSecurityManager(sm);
  }
}
