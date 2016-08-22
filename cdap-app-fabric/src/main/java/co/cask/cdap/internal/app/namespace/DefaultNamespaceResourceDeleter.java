/*
 * Copyright © 2016 Cask Data, Inc.
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
import co.cask.cdap.common.security.Impersonator;
import co.cask.cdap.config.DashboardStore;
import co.cask.cdap.config.PreferencesStore;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.runtime.schedule.Scheduler;
import co.cask.cdap.internal.app.services.ApplicationLifecycleService;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Default implementation of {@link NamespaceResourceDeleter}.
 */
public class DefaultNamespaceResourceDeleter implements NamespaceResourceDeleter {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultNamespaceResourceDeleter.class);

  private final Impersonator impersonator;
  private final Store store;
  private final PreferencesStore preferencesStore;
  private final DashboardStore dashboardStore;
  private final DatasetFramework dsFramework;
  private final QueueAdmin queueAdmin;
  private final StreamAdmin streamAdmin;
  private final MetricStore metricStore;
  private final Scheduler scheduler;
  private final ApplicationLifecycleService applicationLifecycleService;
  private final ArtifactRepository artifactRepository;
  private final StorageProviderNamespaceAdmin storageProviderNamespaceAdmin;

  @Inject
  DefaultNamespaceResourceDeleter(Impersonator impersonator, Store store, PreferencesStore preferencesStore,
                                  DashboardStore dashboardStore, DatasetFramework dsFramework, QueueAdmin queueAdmin,
                                  StreamAdmin streamAdmin, MetricStore metricStore, Scheduler scheduler,
                                  ApplicationLifecycleService applicationLifecycleService,
                                  ArtifactRepository artifactRepository,
                                  StorageProviderNamespaceAdmin storageProviderNamespaceAdmin) {
    this.impersonator = impersonator;
    this.store = store;
    this.preferencesStore = preferencesStore;
    this.dashboardStore = dashboardStore;
    this.dsFramework = dsFramework;
    this.queueAdmin = queueAdmin;
    this.streamAdmin = streamAdmin;
    this.metricStore = metricStore;
    this.scheduler = scheduler;
    this.applicationLifecycleService = applicationLifecycleService;
    this.artifactRepository = artifactRepository;
    this.storageProviderNamespaceAdmin = storageProviderNamespaceAdmin;
  }


  @Override
  public void deleteResources(NamespaceMeta namespaceMeta) throws Exception {
    final NamespaceId namespaceId = namespaceMeta.getNamespaceId();

    // Delete Preferences associated with this namespace
    preferencesStore.deleteProperties(namespaceId.getNamespace());
    // Delete all dashboards associated with this namespace
    dashboardStore.delete(namespaceId.getNamespace());
    // Delete all applications
    applicationLifecycleService.removeAll(namespaceId.toId());
    // Delete all the schedules
    scheduler.deleteAllSchedules(namespaceId.toId());
    // Delete datasets and modules
    dsFramework.deleteAllInstances(namespaceId.toId());
    dsFramework.deleteAllModules(namespaceId.toId());
    // Delete queues and streams data
    queueAdmin.dropAllInNamespace(namespaceId.toId());
    streamAdmin.dropAllInNamespace(namespaceId.toId());
    // Delete all meta data
    store.removeAll(namespaceId.toId());

    deleteMetrics(namespaceId);
    // delete all artifacts in the namespace
    artifactRepository.clear(namespaceId);

    LOG.info("All data for namespace '{}' deleted.", namespaceId);

    // Delete the namespace itself, only if it is a non-default namespace. This is because we do not allow users to
    // create default namespace, and hence deleting it may cause undeterministic behavior.
    // Another reason for not deleting the default namespace is that we do not want to call a delete on the default
    // namespace in the storage provider (Hive, HBase, etc), since we re-use their default namespace.
    if (!NamespaceId.DEFAULT.equals(namespaceId)) {
      impersonator.doAs(namespaceMeta, new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          // Delete namespace in storage providers
          storageProviderNamespaceAdmin.delete(namespaceId);
          return null;
        }
      });
    }
  }

  private void deleteMetrics(NamespaceId namespaceId) throws Exception {
    long endTs = System.currentTimeMillis() / 1000;
    Map<String, String> tags = new HashMap<>();
    tags.put(Constants.Metrics.Tag.NAMESPACE, namespaceId.getNamespace());
    MetricDeleteQuery deleteQuery = new MetricDeleteQuery(0, endTs, tags);
    metricStore.delete(deleteQuery);
  }
}
