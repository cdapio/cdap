/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app.deploy.pipeline;

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.FlowletConnection;
import co.cask.cdap.api.flow.FlowletDefinition;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.api.service.http.HttpServiceHandlerSpecification;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.data2.registry.UsageRegistry;
import co.cask.cdap.internal.app.DefaultApplicationSpecification;
import co.cask.cdap.pipeline.AbstractStage;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.security.impersonation.OwnerAdmin;
import com.google.common.base.Predicate;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class ApplicationRegistrationStage extends AbstractStage<ApplicationWithPrograms> {

  private final Store store;
  private final UsageRegistry usageRegistry;
  private final OwnerAdmin ownerAdmin;

  public ApplicationRegistrationStage(Store store, UsageRegistry usageRegistry, OwnerAdmin ownerAdmin) {
    super(TypeToken.of(ApplicationWithPrograms.class));
    this.store = store;
    this.usageRegistry = usageRegistry;
    this.ownerAdmin = ownerAdmin;
  }

  @Override
  public void process(ApplicationWithPrograms input) throws Exception {
    ApplicationSpecification applicationSpecification = input.getSpecification();
    if (!input.canUpdateSchedules()) {
      applicationSpecification = getApplicationSpecificationWithExistingSchedules(input);
    }
    Collection<ApplicationId> allAppVersionsAppIds = store.getAllAppVersionsAppIds(input.getApplicationId());
    boolean ownerAdded = addOwnerIfRequired(input, allAppVersionsAppIds);
    try {
      store.addApplication(input.getApplicationId(), applicationSpecification);
    } catch (Exception e) {
      // if we failed to store the app spec cleanup the owner if it was added in this call
      if (ownerAdded) {
        ownerAdmin.delete(input.getApplicationId());
      }
      // propagate the exception
      throw e;
    }
    registerDatasets(input);
    emit(input);
  }

  /**
   * uses the existing app spec schedules, however if workflows are deleted in the new app spec,
   * we want to remove the schedules assigned to those deleted workflow from the existing app spec schedules.
   * construct and return a app spec based on this filtered schedules and programs from new app spec.
   * @param input
   * @return {@link ApplicationSpecification} updated spec.
   */
  private ApplicationSpecification getApplicationSpecificationWithExistingSchedules(ApplicationWithPrograms input) {
    Map<String, ScheduleSpecification> filteredExistingSchedules = new HashMap<>();

    if (input.getExistingAppSpec() != null) {
      final Set<String> deletedWorkflows =
        Maps.difference(input.getExistingAppSpec().getWorkflows(), input.getSpecification().getWorkflows()).
          entriesOnlyOnLeft().keySet();

      // predicate to filter schedules if their workflow is deleted
      Predicate<Map.Entry<String, ScheduleSpecification>> byScheduleProgramStatus =
        new Predicate<Map.Entry<String, ScheduleSpecification>>() {
          @Override
          public boolean apply(Map.Entry<String, ScheduleSpecification> input) {
            return !deletedWorkflows.contains(input.getValue().getProgram().getProgramName());
          }
        };

      filteredExistingSchedules = Maps.filterEntries(input.getExistingAppSpec().getSchedules(),
                                                     byScheduleProgramStatus);
    }

    ApplicationSpecification newSpecification = input.getSpecification();
    return new DefaultApplicationSpecification(newSpecification.getName(), newSpecification.getAppVersion(),
                                               newSpecification.getDescription(), newSpecification.getConfiguration(),
                                               newSpecification.getArtifactId(), newSpecification.getStreams(),
                                               newSpecification.getDatasetModules(), newSpecification.getDatasets(),
                                               newSpecification.getFlows(), newSpecification.getMapReduce(),
                                               newSpecification.getSpark(), newSpecification.getWorkflows(),
                                               newSpecification.getServices(), filteredExistingSchedules,
                                               newSpecification.getProgramSchedules(),
                                               newSpecification.getWorkers(), newSpecification.getPlugins());
  }

  // adds owner information for the application if this is the first version of the application
  private boolean addOwnerIfRequired(ApplicationWithPrograms input, Collection<ApplicationId> allAppVersionsAppIds)
    throws IOException, AlreadyExistsException {
    // if allAppVersionsAppIds.isEmpty() is true that means this app is an entirely new app and no other version
    // exists so we should add the owner information in owner store if one was provided
    if (allAppVersionsAppIds.isEmpty() && input.getOwnerPrincipal() != null) {
      ownerAdmin.add(input.getApplicationId(), input.getOwnerPrincipal());
      return true;
    }
    return false;
  }

  // Register dataset usage, based upon the program specifications.
  // Note that worker specifications' datasets are not registered upon app deploy because the useDataset of the
  // WorkerConfigurer is deprecated. Workers' access to datasets is aimed to be completely dynamic. Other programs are
  // moving in this direction.
  // Also, SparkSpecifications are the same in that a Spark program's dataset access is completely dynamic.
  private void registerDatasets(ApplicationWithPrograms input) {
    ApplicationSpecification appSpec = input.getSpecification();
    ApplicationId appId = input.getApplicationId();
    NamespaceId namespaceId = appId.getParent();

    for (FlowSpecification flow : appSpec.getFlows().values()) {
      ProgramId programId = appId.flow(flow.getName());
      for (FlowletConnection connection : flow.getConnections()) {
        if (connection.getSourceType().equals(FlowletConnection.Type.STREAM)) {
          usageRegistry.register(programId, namespaceId.stream(connection.getSourceName()));
        }
      }
      for (FlowletDefinition flowlet : flow.getFlowlets().values()) {
        for (String dataset : flowlet.getDatasets()) {
          usageRegistry.register(programId, namespaceId.dataset(dataset));
        }
      }
    }

    for (MapReduceSpecification program : appSpec.getMapReduce().values()) {
      ProgramId programId = appId.mr(program.getName());
      for (String dataset : program.getDataSets()) {
        usageRegistry.register(programId, namespaceId.dataset(dataset));
      }
    }

    for (SparkSpecification sparkSpec : appSpec.getSpark().values()) {
      ProgramId programId = appId.spark(sparkSpec.getName());
      for (String dataset : sparkSpec.getDatasets()) {
        usageRegistry.register(programId, namespaceId.dataset(dataset));
      }
    }

    for (ServiceSpecification serviceSpecification : appSpec.getServices().values()) {
      ProgramId programId = appId.service(serviceSpecification.getName());
      for (HttpServiceHandlerSpecification handlerSpecification : serviceSpecification.getHandlers().values()) {
        for (String dataset : handlerSpecification.getDatasets()) {
          usageRegistry.register(programId, namespaceId.dataset(dataset));
        }
      }
    }
  }
}
