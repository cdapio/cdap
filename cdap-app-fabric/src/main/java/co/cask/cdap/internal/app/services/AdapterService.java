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
import co.cask.cdap.common.AdapterNotFoundException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.internal.app.runtime.schedule.Scheduler;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.proto.AdapterConfig;
import co.cask.cdap.proto.AdapterStatus;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.templates.AdapterDefinition;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;

/**
 * Service that manages lifecycle of Adapters.
 */
public class AdapterService {
  private static final Logger LOG = LoggerFactory.getLogger(AdapterService.class);

  private static final Function<RunRecordMeta, RunRecord> CONVERT_TO_RUN_RECORD =
    new Function<RunRecordMeta, RunRecord>() {
      @Override
      public RunRecord apply(RunRecordMeta input) {
        return new RunRecord(input);
      }
    };

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
   * Retrieves the {@link AdapterConfig} specified by the name in a given namespace.
   *
   * @param namespace namespace to lookup the adapter
   * @param adapterName name of the adapter
   * @return requested {@link AdapterConfig} or null if no such AdapterInfo exists
   * @throws AdapterNotFoundException if the requested adapter is not found
   */
  public AdapterDefinition getAdapter(Id.Namespace namespace, String adapterName) throws AdapterNotFoundException {
    AdapterDefinition adapterSpec = store.getAdapter(namespace, adapterName);
    if (adapterSpec == null) {
      throw new AdapterNotFoundException(Id.Adapter.from(namespace, adapterName));
    }
    return adapterSpec;
  }

  /**
   * Retrieves the status of an Adapter specified by the name in a given namespace.
   *
   * @param namespace namespace to lookup the adapter
   * @param adapterName name of the adapter
   * @return requested Adapter's status
   * @throws AdapterNotFoundException if the requested adapter is not found
   */
  public AdapterStatus getAdapterStatus(Id.Namespace namespace, String adapterName) throws AdapterNotFoundException {
    AdapterStatus adapterStatus = store.getAdapterStatus(namespace, adapterName);
    if (adapterStatus == null) {
      throw new AdapterNotFoundException(Id.Adapter.from(namespace, adapterName));
    }
    return adapterStatus;
  }

  /**
   * Get all adapters in a given namespace.
   *
   * @param namespace the namespace to look up the adapters
   * @return {@link Collection} of {@link AdapterConfig}
   */
  public Collection<AdapterDefinition> getAdapters(Id.Namespace namespace) {
    return store.getAllAdapters(namespace);
  }

  /**
   * Fetch RunRecords for a given adapter.
   *
   * @param namespace namespace in which adapter is deployed
   * @param adapterName name of the adapter
   * @param status {@link ProgramRunStatus} status of the program running/completed/failed or all
   * @param start fetch run history that has started after the startTime in seconds
   * @param end fetch run history that has started before the endTime in seconds
   * @param limit max number of entries to fetch for this history call
   * @return list of {@link RunRecord}
   * @throws NotFoundException if adapter is not found
   */
  public List<RunRecord> getRuns(Id.Namespace namespace, String adapterName, ProgramRunStatus status,
                                 long start, long end, int limit) throws NotFoundException {
    Id.Program program = getProgramId(namespace, adapterName);

    return Lists.transform(store.getRuns(program, status, start, end, limit, adapterName), CONVERT_TO_RUN_RECORD);
  }

  @VisibleForTesting
  private Id.Program getProgramId(Id.Namespace namespace, String adapterName) throws NotFoundException {
    return getProgramId(namespace, getAdapter(namespace, adapterName));
  }

  private Id.Program getProgramId(Id.Namespace namespace, AdapterDefinition adapterSpec) throws NotFoundException {
    Id.Application appId = Id.Application.from(namespace, adapterSpec.getTemplate());
    ApplicationSpecification appSpec = store.getApplication(appId);
    if (appSpec == null) {
      throw new NotFoundException(appId);
    }
    return adapterSpec.getProgram();
  }

  /**
   * Upgrades to cdap 3.2, which has removed adapters and application templates. Deletes all adapters,
   * adapter related schedules, and application templates.
   */
  public void upgrade() throws Exception {
    for (NamespaceMeta namespaceMeta : store.listNamespaces()) {
      Id.Namespace namespace = Id.Namespace.from(namespaceMeta.getName());
      for (AdapterDefinition adapterDefinition : getAdapters(namespace)) {

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
