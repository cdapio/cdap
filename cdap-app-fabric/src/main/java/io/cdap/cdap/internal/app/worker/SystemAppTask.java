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

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.inject.AbstractModule;
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
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.LocalLocationModule;
import io.cdap.cdap.common.guice.SupplierProviderBridge;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactManagerFactory;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.Artifacts;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.worker.sidecar.ArtifactLocalizerClient;
import io.cdap.cdap.master.environment.MasterEnvironments;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.messaging.guice.MessagingClientModule;
import io.cdap.cdap.metadata.PreferencesFetcher;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.guice.SecureStoreClientModule;
import io.cdap.cdap.security.impersonation.EntityImpersonator;
import io.cdap.cdap.security.impersonation.Impersonator;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * SystemAppTask launches a task created by system app with application classloader
 */
public class SystemAppTask implements RunnableTask {

  private static final Logger LOG = LoggerFactory.getLogger(SystemAppTask.class);
  private static final Gson GSON = new Gson();

  private final CConfiguration cConf;

  @Inject
  SystemAppTask(CConfiguration cConf) {
    this.cConf = cConf;
  }

  @Override
  public void run(RunnableTaskContext context) throws Exception {
    ArtifactId systemAppArtifactId = context.getArtifactId();
    if (systemAppArtifactId == null) {
      throw new IllegalArgumentException("Missing artifactId from the system app task request");
    }
    LOG.debug("Received system app task for artifact {}", systemAppArtifactId);

    Injector injector = createInjector(cConf);

    ArtifactRepository artifactRepository = injector.getInstance(ArtifactRepository.class);
    Impersonator impersonator = injector.getInstance(Impersonator.class);

    String systemAppNamespace = context.getNamespace();
    Id.Artifact artifactId = Id.Artifact
      .from(Id.Namespace.from(systemAppNamespace), systemAppArtifactId.getName(),
            systemAppArtifactId.getVersion());
    ArtifactLocalizerClient localizerClient = injector.getInstance(ArtifactLocalizerClient.class);
    File artifactLocation = localizerClient
      .getUnpackedArtifactLocation(
        Artifacts.toProtoArtifactId(new NamespaceId(systemAppNamespace), systemAppArtifactId));

    EntityImpersonator classLoaderImpersonator = new EntityImpersonator(artifactId.toEntityId(), impersonator);

    try (CloseableClassLoader artifactClassLoader =
           artifactRepository.createArtifactClassLoader(Locations.toLocation(artifactLocation),
                                                        classLoaderImpersonator);
         SystemAppTaskContext systemAppTaskContext = buildTaskSystemAppContext(injector, systemAppNamespace,
                                                                               systemAppArtifactId,
                                                                               artifactClassLoader)) {
      RunnableTaskRequest taskRequest = context.getEmbeddedRequest();
      String taskClassName = taskRequest.getClassName();
      if (taskClassName == null) {
        LOG.debug("No system app task to execute");
        return;
      }
      LOG.debug("Requested to run system app task {}", taskClassName);

      Class<?> clazz = artifactClassLoader.loadClass(taskClassName);
      if (!(RunnableTask.class.isAssignableFrom(clazz))) {
        throw new ClassCastException(String.format("%s is not a RunnableTask", taskClassName));
      }

      LOG.debug("Launching system app task {}", taskClassName);
      RunnableTask runnableTask = (RunnableTask) injector.getInstance(clazz);
      RunnableTaskContext runnableTaskContext = new RunnableTaskContext(taskRequest.getParam().getSimpleParam(), null,
                                                                        null, null, systemAppTaskContext) {
        @Override
        public void writeResult(byte[] data) throws IOException {
          context.writeResult(data);
        }

        @Override
        public void setTerminateOnComplete(boolean terminate) {
          context.setTerminateOnComplete(terminate);
        }

        @Override
        public boolean isTerminateOnComplete() {
          return context.isTerminateOnComplete();
        }
      };

      runnableTask.run(runnableTaskContext);
      LOG.debug("System app task completed {}", taskClassName);
    }
  }

  @VisibleForTesting
  static Injector createInjector(CConfiguration cConf) {
    return Guice.createInjector(
      new ConfigModule(cConf),
      new MessagingClientModule(),
      new LocalLocationModule(),
      new SecureStoreClientModule(),
      new AuthenticationContextModules().getMasterModule(),
      new SystemAppModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          MasterEnvironment masterEnv = MasterEnvironments.getMasterEnvironment();
          if (masterEnv != null) {
            bind(DiscoveryService.class)
              .toProvider(new SupplierProviderBridge<>(masterEnv.getDiscoveryServiceSupplier()));
            bind(DiscoveryServiceClient.class)
              .toProvider(new SupplierProviderBridge<>(masterEnv.getDiscoveryServiceClientSupplier()));
          }
        }
      });
  }

  private SystemAppTaskContext buildTaskSystemAppContext(Injector injector, String systemAppNamespace,
                                                         ArtifactId artifactId, ClassLoader artifactClassLoader) {
    PreferencesFetcher preferencesFetcher = injector.getInstance(PreferencesFetcher.class);
    PluginFinder pluginFinder = injector.getInstance(PluginFinder.class);
    SecureStore secureStore = injector.getInstance(SecureStore.class);
    ArtifactManagerFactory artifactManagerFactory = injector.getInstance(ArtifactManagerFactory.class);
    RemoteClientFactory remoteClientFactory = injector.getInstance(RemoteClientFactory.class);
    return new DefaultSystemAppTaskContext(injector.getInstance(CConfiguration.class),
                                           preferencesFetcher, pluginFinder, secureStore, systemAppNamespace,
                                           artifactId, artifactClassLoader, artifactManagerFactory,
                                           Constants.Service.TASK_WORKER, remoteClientFactory);
  }
}
