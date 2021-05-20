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
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.cdap.cdap.api.artifact.CloseableClassLoader;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreManager;
import io.cdap.cdap.api.service.worker.RunnableTask;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.api.service.worker.RunnableTaskSystemAppContext;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDetail;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepositoryReader;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.metadata.PreferencesFetcher;
import io.cdap.cdap.security.impersonation.EntityImpersonator;
import io.cdap.cdap.security.impersonation.Impersonator;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.discovery.DiscoveryServiceClient;

/**
 * SystemAppTask launches a task created by system app with application classloader
 */
public class SystemAppTask implements RunnableTask {

  private CConfiguration cConf;
  private Configuration hConf;
  private ArtifactRepositoryReader artifactRepositoryReader;
  private ArtifactRepository artifactRepository;
  private Impersonator impersonator;
  private DatasetFramework dsFramework;
  private SecureStoreManager secureStoreManager;
  private MessagingService messagingService;
  private RetryStrategy retryStrategy;
  private NamespaceQueryAdmin namespaceQueryAdmin;
  private PreferencesFetcher preferencesFetcher;
  private PluginFinder pluginFinder;
  private DiscoveryServiceClient discoveryServiceClient;
  private SecureStore secureStore;


  @Inject
  SystemAppTask(CConfiguration cConf, Configuration hConf, ArtifactRepositoryReader artifactRepositoryReader,
                ArtifactRepository artifactRepository, Impersonator impersonator, DatasetFramework dsFramework,
                SecureStoreManager secureStoreManager, MessagingService messagingService,
                NamespaceQueryAdmin namespaceQueryAdmin, PreferencesFetcher preferencesFetcher,
                PluginFinder pluginFinder, DiscoveryServiceClient discoveryServiceClient, SecureStore secureStore) {
    this.cConf = cConf;
    this.hConf = hConf;
    this.artifactRepositoryReader = artifactRepositoryReader;
    this.artifactRepository = artifactRepository;
    this.impersonator = impersonator;
    this.dsFramework = dsFramework;
    this.secureStoreManager = secureStoreManager;
    this.messagingService = messagingService;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.preferencesFetcher = preferencesFetcher;
    this.pluginFinder = pluginFinder;
    this.discoveryServiceClient = discoveryServiceClient;
    this.secureStore = secureStore;
    this.retryStrategy = RetryStrategies.fromConfiguration(cConf, Constants.Service.TASK_WORKER + ".");
  }

  @Override
  public void run(RunnableTaskContext context) throws Exception {
    RunnableTaskRequest taskRequest = context.getDelegateTaskRequest();
    Injector injector = Guice.createInjector(new SystemAppModule(cConf, hConf));
    String namespace = taskRequest.getNamespace();
    Id.Artifact artifactId = Id.Artifact
      .from(Id.Namespace.SYSTEM, taskRequest.getArtifactId().getName(),
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

    RunnableTaskSystemAppContext systemAppContext =
      new DefaultRunnableTaskSystemAppContext(cConf, dsFramework, namespace, secureStoreManager, messagingService,
                                              retryStrategy, namespaceQueryAdmin, preferencesFetcher,
                                              artifactClassLoader, pluginFinder, taskRequest.getArtifactId(),
                                              discoveryServiceClient, secureStore);
    RunnableTask runnableTask = (RunnableTask) obj;
    RunnableTaskContext runnableTaskContext = RunnableTaskContext.getBuilder().
      withParam(taskRequest.getParam()).
      withArtifactClassLoader(artifactClassLoader).
      withSystemAppContext(systemAppContext).build();
    runnableTask.run(runnableTaskContext);
    context.writeResult(runnableTaskContext.getResult());
  }
}
