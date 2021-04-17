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
import io.cdap.cdap.api.task.RunnableTaskRequest;

/**
 * RunnableTaskLauncher launches a runnable task.
 */
public class RunnableTaskLauncher {

  public String launchRunnableTask(RunnableTaskRequest request) throws Exception {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    Class<?> cClass = classLoader.loadClass(request.getClassName());
    Object obj = cClass.getDeclaredConstructor().newInstance();
    if (!(obj instanceof RunnableTask)) {
      throw new ClassCastException(String.format("%s is not a RunnableTask", request.getClassName()));
    }
    RunnableTask runnableTask = (RunnableTask) obj;
    if (runnableTask.start().get() != Service.State.RUNNING) {
      throw new Exception(String.format("service %s failed to start", request.getClassName()));
    }
    return runnableTask.runTask(request.getParam());
  }
}
