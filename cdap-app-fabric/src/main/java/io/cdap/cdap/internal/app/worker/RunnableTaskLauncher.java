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

import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDetail;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepositoryReader;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.security.impersonation.EntityImpersonator;
import io.cdap.cdap.security.impersonation.Impersonator;

import java.lang.reflect.Method;

/**
 * RunnableTaskLauncher launches a runnable task.
 */
public class RunnableTaskLauncher {
  private final CConfiguration cConfig;
  private final ArtifactRepositoryReader artifactRepositoryReader;
  private final ArtifactRepository artifactRepository;
  private final Impersonator impersonator;

  public RunnableTaskLauncher(CConfiguration cConfig,
                              ArtifactRepositoryReader artifactRepositoryReader,
                              ArtifactRepository artifactRepository,
                              Impersonator impersonator) {
    this.cConfig = cConfig;
    this.artifactRepositoryReader = artifactRepositoryReader;
    this.artifactRepository = artifactRepository;
    this.impersonator = impersonator;
  }

  public byte[] launchRunnableTask(RunnableTaskRequest request) throws Exception {
    if (request.artifactId == null) {
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
    } else {
      Id.Artifact artifactId = request.artifactId;
      ArtifactDetail artifactDetail =
        artifactRepositoryReader.getArtifact(artifactId);
      EntityImpersonator classLoaderImpersonator = new EntityImpersonator(
        new ArtifactId(artifactId.getNamespace().getId(), artifactId.getName(), artifactId.getVersion().getVersion()),
        impersonator);
      ClassLoader classLoader = artifactRepository.createArtifactClassLoader(
        artifactDetail.getDescriptor().getLocation(),
        classLoaderImpersonator);

      Class<?> clazz = classLoader.loadClass(request.className);

      Injector injector = Guice.createInjector(new RunnableTaskModule(cConfig));
      Object obj = injector.getInstance(clazz);

      Class<RunnableTask> runnableTaskClass = (Class<RunnableTask>) classLoader.loadClass(RunnableTask.class.getName());

      Method method = runnableTaskClass.getMethod(RunnableTask.RUN_MEHOD_NAME, java.lang.String.class);

      return (byte[]) method.invoke(obj, new Object[]{request.param});
    }
  }
}
