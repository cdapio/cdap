/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.internal.app.runtime.schedule.Scheduler;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.templates.AdapterDefinition;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service that manages lifecycle of Adapters. Only around for upgrade
 */
@Deprecated
public class AdapterService {
  private static final Logger LOG = LoggerFactory.getLogger(AdapterService.class);

  private final Scheduler scheduler;
  private final Store store;
  private final ApplicationLifecycleService applicationLifecycleService;

  @Inject
  public AdapterService(Scheduler scheduler, Store store,
                        ApplicationLifecycleService applicationLifecycleService) {
    this.scheduler = scheduler;
    this.store = store;
    this.applicationLifecycleService = applicationLifecycleService;
  }

  /**
   * Upgrades to cdap 3.2, which has removed adapters and application templates. Deletes all adapters,
   * adapter related schedules, and application templates.
   */
  public void upgrade() throws Exception {
    for (NamespaceMeta namespaceMeta : store.listNamespaces()) {
      Id.Namespace namespace = Id.Namespace.from(namespaceMeta.getName());
      for (AdapterDefinition adapterDefinition : store.getAllAdapters(namespace)) {

        // try deleting the app template first. Needs to happen before deleting adapters because
        // the only way we know that an application template exists is if there are adapters for it.
        Id.Application appId = Id.Application.from(namespace, adapterDefinition.getTemplate());
        LOG.info("Deleting application template {} in namespace {}.", appId.getId(), appId.getNamespaceId());
        ApplicationSpecification appSpec = store.getApplication(appId);
        // can be null if we failed midway through an early upgrade
        if (appSpec != null) {
          // throws an exception if it fails and halts the upgrade
          applicationLifecycleService.deleteApp(appId, appSpec);
        }

        Id.Program workflowId = adapterDefinition.getProgram();
        // if its a batch adapter, delete the schdule
        if (adapterDefinition.getScheduleSpecification() != null) {
          String scheduleName = adapterDefinition.getScheduleSpecification().getSchedule().getName();
          try {
            scheduler.deleteSchedule(workflowId, SchedulableProgramType.WORKFLOW, scheduleName);
            store.deleteSchedule(workflowId, scheduleName);
          } catch (NotFoundException e) {
            // ok if it's not found, means we're deleting a stopped adapter
          }
        }

        store.removeAdapter(namespace, adapterDefinition.getName());
      }
    }
  }
}
