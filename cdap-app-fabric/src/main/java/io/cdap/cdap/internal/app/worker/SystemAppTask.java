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

import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.CloseableClassLoader;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.service.worker.RunnableTask;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.api.service.worker.SystemAppTaskContext;
import io.cdap.cdap.app.guice.DistributedArtifactManagerModule;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.LocalLocationModule;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDetail;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactManagerFactory;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepositoryReader;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.logging.guice.RemoteLogAppenderModule;
import io.cdap.cdap.messaging.guice.MessagingClientModule;
import io.cdap.cdap.metadata.PreferencesFetcher;
import io.cdap.cdap.security.guice.SecureStoreClientModule;
import io.cdap.cdap.security.impersonation.EntityImpersonator;
import io.cdap.cdap.security.impersonation.Impersonator;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * SystemAppTask launches a task created by system app with application classloader
 */
public class SystemAppTask implements RunnableTask {

  private static final Gson GSON = new Gson();

  private final CConfiguration cConf;

  @Inject
  SystemAppTask(CConfiguration cConf) {
    this.cConf = cConf;
  }

  @Override
  public void run(RunnableTaskContext context) throws Exception {
    Injector injector = createInjector();
    ArtifactRepositoryReader artifactRepositoryReader = injector.getInstance(ArtifactRepositoryReader.class);
    ArtifactRepository artifactRepository = injector.getInstance(ArtifactRepository.class);
    Impersonator impersonator = injector.getInstance(Impersonator.class);
    ArtifactId systemAppArtifactId = context.getArtifactId();
    String systemAppNamespace = context.getNamespace();
    Id.Artifact artifactId = Id.Artifact
      .from(Id.Namespace.from(systemAppNamespace), systemAppArtifactId.getName(),
            systemAppArtifactId.getVersion());
    ArtifactDetail artifactDetail = artifactRepositoryReader.getArtifact(artifactId);
    streamArtifactIfRequired(artifactId, artifactDetail.getDescriptor().getLocation(), artifactRepository);

    EntityImpersonator classLoaderImpersonator = new EntityImpersonator(artifactId.toEntityId(), impersonator);
    try (CloseableClassLoader artifactClassLoader =
           artifactRepository.createArtifactClassLoader(artifactDetail.getDescriptor().getLocation(),
                                                        classLoaderImpersonator);
         SystemAppTaskContext systemAppTaskContext = buildTaskSystemAppContext(injector, systemAppNamespace,
                                                                               systemAppArtifactId,
                                                                               artifactClassLoader)) {
      RunnableTaskRequest taskRequest = GSON.fromJson(context.getParam(), RunnableTaskRequest.class);
      String taskClassName = taskRequest.getClassName();
      Class<?> clazz = artifactClassLoader.loadClass(taskClassName);
      if (!(RunnableTask.class.isAssignableFrom(clazz))) {
        throw new ClassCastException(String.format("%s is not a RunnableTask", taskClassName));
      }

      RunnableTask runnableTask = (RunnableTask) injector.getInstance(clazz);
      RunnableTaskContext runnableTaskContext = RunnableTaskContext.getBuilder().
        withParam(taskRequest.getParam()).
        withTaskSystemAppContext(systemAppTaskContext).build();
      runnableTask.run(runnableTaskContext);
      context.writeResult(runnableTaskContext.getResult());
    }
  }

  private Injector createInjector() {
    return Guice.createInjector(
      new ConfigModule(cConf),
      new RemoteLogAppenderModule(),
      new DistributedArtifactManagerModule(),
      new MessagingClientModule(),
      new LocalLocationModule(),
      new SecureStoreClientModule(),
      new SystemAppModule());
  }

  private SystemAppTaskContext buildTaskSystemAppContext(Injector injector, String systemAppNamespace,
                                                         ArtifactId artifactId, ClassLoader artifactClassLoader) {
    PreferencesFetcher preferencesFetcher = injector.getInstance(PreferencesFetcher.class);
    PluginFinder pluginFinder = injector.getInstance(PluginFinder.class);
    DiscoveryServiceClient discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
    SecureStore secureStore = injector.getInstance(SecureStore.class);
    ArtifactManagerFactory artifactManagerFactory = injector.getInstance(ArtifactManagerFactory.class);
    return new DefaultSystemAppTaskContext(cConf, preferencesFetcher, pluginFinder, discoveryServiceClient,
                                           secureStore, systemAppNamespace, artifactId, artifactClassLoader,
                                           artifactManagerFactory, Constants.Service.TASK_WORKER);
  }

  private void streamArtifactIfRequired(Id.Artifact artifactId, Location artifactLocation,
                                        ArtifactRepository artifactRepository) throws IOException, NotFoundException {
    if (artifactLocation.exists()) {
      return;
    }
    try (InputStream is = artifactRepository.newInputStream(artifactId);
         OutputStream os = artifactLocation.getOutputStream()) {
      ByteStreams.copy(is, os);
    }
  }
}
