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
import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.api.service.http.HttpServiceHandlerSpecification;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.common.kerberos.OwnerAdmin;
import co.cask.cdap.data2.registry.UsageRegistry;
import co.cask.cdap.pipeline.AbstractStage;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.KerberosPrincipalId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.reflect.TypeToken;

import java.io.IOException;
import java.util.Collection;
import javax.annotation.Nullable;

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
    Collection<ApplicationId> allAppVersionsAppIds = store.getAllAppVersionsAppIds(input.getApplicationId());
    // if allAppVersionsAppIds.isEmpty() is true that means this app is an entirely new app and no other version
    // exists so we should add the owner information in owner store if one was provided
    if (allAppVersionsAppIds.isEmpty() && input.getOwnerPrincipal() != null) {
      addOwner(input.getApplicationId(), input.getOwnerPrincipal());
    }

    store.addApplication(input.getApplicationId(), input.getSpecification());
    registerDatasets(input);
    emit(input);
  }

  /**
   * Adds the application owner information to the owner store
   */
  private void addOwner(ApplicationId entityId,
                        @Nullable KerberosPrincipalId specifiedOwnerPrincipal)
    throws IOException, AlreadyExistsException {
      ownerAdmin.add(entityId, specifiedOwnerPrincipal);
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
