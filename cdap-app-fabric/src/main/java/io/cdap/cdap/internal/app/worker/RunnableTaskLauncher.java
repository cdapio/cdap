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
import io.cdap.cdap.common.conf.CConfiguration;
<<<<<<<HEAD
import io.cdap.cdap.internal.worker.api.RunnableTask;
import io.cdap.cdap.internal.worker.api.RunnableTaskContext;
import io.cdap.cdap.internal.worker.api.RunnableTaskRequest;
=======
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDetail;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepositoryReader;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.security.impersonation.EntityImpersonator;
import io.cdap.cdap.security.impersonation.Impersonator;

import java.lang.reflect.Method;

/**
 * RunnableTaskLauncher launches a {@link RunnableTask} by loading its class and calling its run method.
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
    ClassLoader classLoader = getClassLoader(request.getArtifactId(), request.getNamespace());
    Class<?> clazz = classLoader.loadClass(request.getClassName());
    Injector injector = Guice.createInjector(new RunnableTaskModule(cConfig));
    Object obj = injector.getInstance(clazz);

    if (!(obj instanceof RunnableTask)) {
      throw new ClassCastException(String.format("%s is not a RunnableTask", request.getClassName()));
    }
    RunnableTask runnableTask = (RunnableTask) obj;
    RunnableTaskContext runnableTaskContext = new RunnableTaskContext(request.getParam(), classLoader);
    runnableTask.run(runnableTaskContext);
    return runnableTaskContext.getResult();
  }

  private ClassLoader getClassLoader(io.cdap.cdap.api.artifact.ArtifactId requestArtifactId,
                                     String namespace) throws Exception {
    if (requestArtifactId == null) {
      ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
      return classLoader == null ? getClass().getClassLoader() : classLoader;
    }
    Id.Artifact artifactId = Id.Artifact
      .from(Id.Namespace.from(namespace), requestArtifactId.getName(), requestArtifactId.getVersion());
    ArtifactDetail artifactDetail = artifactRepositoryReader.getArtifact(artifactId);
    EntityImpersonator classLoaderImpersonator = new EntityImpersonator(artifactId.toEntityId(), impersonator);
    return artifactRepository.createArtifactClassLoader(artifactDetail.getDescriptor().getLocation(),
                                                        classLoaderImpersonator);
  }
}
