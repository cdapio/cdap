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
import io.cdap.cdap.api.artifact.CloseableClassLoader;
import io.cdap.cdap.api.service.worker.RunnableTask;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDetail;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepositoryReader;
import io.cdap.cdap.security.impersonation.EntityImpersonator;
import io.cdap.cdap.security.impersonation.Impersonator;
import org.apache.hadoop.conf.Configuration;

/**
 * SystemAppTask launches a task created by system app with application classloader
 */
public class SystemAppTask implements RunnableTask {

  private CConfiguration cConf;
  private Configuration hConf;
  private ArtifactRepositoryReader artifactRepositoryReader;
  private ArtifactRepository artifactRepository;
  private Impersonator impersonator;
  private RunnableTaskRequest taskRequest;

  SystemAppTask(CConfiguration cConf, Configuration hConf, ArtifactRepositoryReader artifactRepositoryReader,
                ArtifactRepository artifactRepository, Impersonator impersonator, RunnableTaskRequest taskRequest) {
    this.cConf = cConf;
    this.hConf = hConf;
    this.artifactRepositoryReader = artifactRepositoryReader;
    this.artifactRepository = artifactRepository;
    this.impersonator = impersonator;
    this.taskRequest = taskRequest;
  }

  @Override
  public void run(RunnableTaskContext context) throws Exception {
    Injector injector = Guice.createInjector(new SystemAppModule(cConf, hConf));
    Id.Artifact artifactId = Id.Artifact
      .from(Id.Namespace.from(taskRequest.getNamespace()), taskRequest.getArtifactId().getName(),
            taskRequest.getArtifactId().getVersion());
    ArtifactDetail artifactDetail = artifactRepositoryReader.getArtifact(artifactId);
    EntityImpersonator classLoaderImpersonator = new EntityImpersonator(artifactId.toEntityId(), impersonator);
    CloseableClassLoader artifactClassLoader = artifactRepository
      .createArtifactClassLoader(artifactDetail.getDescriptor().getLocation(),
                                 classLoaderImpersonator);
    String taskClassName = taskRequest.getClassName();
    Class<?> clazz = artifactClassLoader.loadClass(taskClassName);
    Object obj = injector.getInstance(clazz);
    if (!(obj instanceof RunnableTask)) {
      throw new ClassCastException(String.format("%s is not a RunnableTask", taskClassName));
    }

    RunnableTask runnableTask = (RunnableTask) obj;
    RunnableTaskContext runnableTaskContext = new RunnableTaskContext(taskRequest.getParam(), artifactClassLoader);
    runnableTask.run(runnableTaskContext);
    context.writeResult(runnableTaskContext.getResult());
  }
}
