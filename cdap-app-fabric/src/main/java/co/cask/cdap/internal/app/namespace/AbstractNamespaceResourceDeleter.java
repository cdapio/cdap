/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app.namespace;

import co.cask.cdap.api.metrics.MetricDeleteQuery;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.config.DashboardStore;
import co.cask.cdap.config.PreferencesService;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.services.ApplicationLifecycleService;
import co.cask.cdap.internal.profile.ProfileService;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.TopicId;
import co.cask.cdap.security.impersonation.Impersonator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Abstract class for deleting namespace components
 */
public abstract class AbstractNamespaceResourceDeleter implements NamespaceResourceDeleter {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractNamespaceResourceDeleter.class);

  private final Impersonator impersonator;
  private final Store store;
  private final PreferencesService preferencesService;
  private final DashboardStore dashboardStore;
  private final DatasetFramework dsFramework;
  private final QueueAdmin queueAdmin;
  private final MetricStore metricStore;
  private final ApplicationLifecycleService applicationLifecycleService;
  private final ArtifactRepository artifactRepository;
  private final StorageProviderNamespaceAdmin storageProviderNamespaceAdmin;
  private final MessagingService messagingService;
  private final ProfileService profileService;


  AbstractNamespaceResourceDeleter(Impersonator impersonator, Store store, PreferencesService preferencesService,
                                   DashboardStore dashboardStore, DatasetFramework dsFramework, QueueAdmin queueAdmin,
                                   MetricStore metricStore,
                                   ApplicationLifecycleService applicationLifecycleService,
                                   ArtifactRepository artifactRepository,
                                   StorageProviderNamespaceAdmin storageProviderNamespaceAdmin,
                                   MessagingService messagingService, ProfileService profileService) {
    this.impersonator = impersonator;
    this.store = store;
    this.preferencesService = preferencesService;
    this.dashboardStore = dashboardStore;
    this.dsFramework = dsFramework;
    this.queueAdmin = queueAdmin;
    this.metricStore = metricStore;
    this.applicationLifecycleService = applicationLifecycleService;
    this.artifactRepository = artifactRepository;
    this.storageProviderNamespaceAdmin = storageProviderNamespaceAdmin;
    this.messagingService = messagingService;
    this.profileService = profileService;
  }

  protected abstract void deleteStreams(NamespaceId namespaceId) throws Exception;

  @Override
  public void deleteResources(NamespaceMeta namespaceMeta) throws Exception {
    final NamespaceId namespaceId = namespaceMeta.getNamespaceId();

    // Delete Preferences associated with this namespace
    preferencesService.deleteProperties(namespaceId);
    // Delete all dashboards associated with this namespace
    dashboardStore.delete(namespaceId.getNamespace());
    // Delete all applications
    applicationLifecycleService.removeAll(namespaceId);
    // Delete datasets and modules
    dsFramework.deleteAllInstances(namespaceId);
    dsFramework.deleteAllModules(namespaceId);
    // Delete queues and streams data
    queueAdmin.dropAllInNamespace(namespaceId);

    // Delete all the streams in namespace
    deleteStreams(namespaceId);

    // Delete all meta data
    store.removeAll(namespaceId);

    deleteMetrics(namespaceId);
    // delete all artifacts in the namespace
    artifactRepository.clear(namespaceId);

    // delete all profiles in the namespace
    profileService.deleteAllProfiles(namespaceId);

    // delete all messaging topics in the namespace
    for (TopicId topicId : messagingService.listTopics(namespaceId)) {
      messagingService.deleteTopic(topicId);
    }

    LOG.info("All data for namespace '{}' deleted.", namespaceId);

    // Delete the namespace itself, only if it is a non-default namespace. This is because we do not allow users to
    // create default namespace, and hence deleting it may cause undeterministic behavior.
    // Another reason for not deleting the default namespace is that we do not want to call a delete on the default
    // namespace in the storage provider (Hive, HBase, etc), since we re-use their default namespace.
    if (!NamespaceId.DEFAULT.equals(namespaceId)) {
      impersonator.doAs(namespaceId, new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          // Delete namespace in storage providers
          storageProviderNamespaceAdmin.delete(namespaceId);
          return null;
        }
      });
    }
  }

  private void deleteMetrics(NamespaceId namespaceId) {
    long endTs = System.currentTimeMillis() / 1000;
    Map<String, String> tags = new LinkedHashMap<>();
    tags.put(Constants.Metrics.Tag.NAMESPACE, namespaceId.getNamespace());
    MetricDeleteQuery deleteQuery = new MetricDeleteQuery(0, endTs, Collections.emptySet(), tags,
                                                          new ArrayList<>(tags.keySet()));
    metricStore.delete(deleteQuery);
  }
}
