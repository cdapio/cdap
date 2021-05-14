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

package io.cdap.cdap.internal.app.worker;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.api.service.worker.RunnableTask;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepositoryReader;
import io.cdap.cdap.security.impersonation.Impersonator;
import org.apache.hadoop.conf.Configuration;

/**
 * RunnableTaskLauncher launches a {@link RunnableTask} by loading its class and calling its run method.
 */
public class RunnableTaskLauncher {
  private final CConfiguration cConf;
  private final Configuration hConf;
  private final ArtifactRepositoryReader artifactRepositoryReader;
  private final ArtifactRepository artifactRepository;
  private final Impersonator impersonator;

  public RunnableTaskLauncher(CConfiguration cConf,
                              Configuration hConf,
                              ArtifactRepositoryReader artifactRepositoryReader,
                              ArtifactRepository artifactRepository,
                              Impersonator impersonator) {
    this.cConf = cConf;
    this.hConf = hConf;
    this.artifactRepositoryReader = artifactRepositoryReader;
    this.artifactRepository = artifactRepository;
    this.impersonator = impersonator;
  }

  public byte[] launchRunnableTask(RunnableTaskRequest request) throws Exception {
    if (request.getArtifactId() != null) {
      return launchSystemAppTask(request);
    }
    ClassLoader classLoader = getClassLoader();
    Injector injector = Guice.createInjector(new RunnableTaskModule(cConf));
    Class<?> clazz = classLoader.loadClass(request.getClassName());
    Object obj = injector.getInstance(clazz);
    if (!(obj instanceof RunnableTask)) {
      throw new ClassCastException(String.format("%s is not a RunnableTask", request.getClassName()));
    }
    RunnableTask runnableTask = (RunnableTask) obj;
    RunnableTaskContext runnableTaskContext = new RunnableTaskContext(request.getParam(), classLoader);
    runnableTask.run(runnableTaskContext);
    return runnableTaskContext.getResult();
  }

  private byte[] launchSystemAppTask(RunnableTaskRequest request) throws Exception {
    RunnableTaskContext context = new RunnableTaskContext(request.getParam());
    SystemAppTask systemAppTask = new SystemAppTask(cConf, hConf, artifactRepositoryReader, artifactRepository,
                                                    impersonator, request);
    systemAppTask.run(context);
    return context.getResult();
  }

  private ClassLoader getClassLoader() throws Exception {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    return classLoader == null ? getClass().getClassLoader() : classLoader;
  }
}
