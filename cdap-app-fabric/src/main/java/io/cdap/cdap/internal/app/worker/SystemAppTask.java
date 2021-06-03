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

import com.google.gson.Gson;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.api.artifact.CloseableClassLoader;
import io.cdap.cdap.api.service.worker.RunnableTask;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDetail;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepositoryReader;
import io.cdap.cdap.security.impersonation.EntityImpersonator;
import io.cdap.cdap.security.impersonation.Impersonator;

/**
 * SystemAppTask launches a task created by system app with application classloader
 */
public class SystemAppTask implements RunnableTask {

  private static final Gson GSON = new Gson();

  @Override
  public void run(RunnableTaskContext context) throws Exception {
    RunnableTaskRequest taskRequest = GSON.fromJson(context.getParam(), RunnableTaskRequest.class);
    Injector injector = Guice.createInjector(new SystemAppModule());
    ArtifactRepositoryReader artifactRepositoryReader = injector.getInstance(ArtifactRepositoryReader.class);
    ArtifactRepository artifactRepository = injector.getInstance(ArtifactRepository.class);
    Impersonator impersonator = injector.getInstance(Impersonator.class);
    Id.Artifact artifactId = Id.Artifact
      .from(Id.Namespace.from(taskRequest.getNamespace()), taskRequest.getArtifactId().getName(),
            taskRequest.getArtifactId().getVersion());
    ArtifactDetail artifactDetail = artifactRepositoryReader.getArtifact(artifactId);
    EntityImpersonator classLoaderImpersonator = new EntityImpersonator(artifactId.toEntityId(), impersonator);
    try (CloseableClassLoader artifactClassLoader =
           artifactRepository.createArtifactClassLoader(artifactDetail.getDescriptor().getLocation(),
                                                        classLoaderImpersonator)) {
      String taskClassName = taskRequest.getClassName();
      Class<?> clazz = artifactClassLoader.loadClass(taskClassName);
      if (!(RunnableTask.class.isAssignableFrom(clazz))) {
        throw new ClassCastException(String.format("%s is not a RunnableTask", taskClassName));
      }

      RunnableTask runnableTask = (RunnableTask) injector.getInstance(clazz);
      RunnableTaskContext runnableTaskContext = RunnableTaskContext.getBuilder().
        withParam(taskRequest.getParam()).
        withArtifactClassLoader(artifactClassLoader).build();
      runnableTask.run(runnableTaskContext);
      context.writeResult(runnableTaskContext.getResult());
    }
  }
}
