/*
 * Copyright © 2014 Cask Data, Inc.
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

import co.cask.cdap.api.data.stream.StreamBatchReadable;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.FlowletConnection;
import co.cask.cdap.api.flow.FlowletDefinition;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.registry.UsageRegistry;
import co.cask.cdap.pipeline.AbstractStage;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import com.google.common.reflect.TypeToken;

import java.net.URI;

/**
 *
 */
public class ApplicationRegistrationStage extends AbstractStage<ApplicationWithPrograms> {

  private final Store store;
  private final UsageRegistry usageRegistry;

  public ApplicationRegistrationStage(Store store, UsageRegistry usageRegistry) {
    super(TypeToken.of(ApplicationWithPrograms.class));
    this.store = store;
    this.usageRegistry = usageRegistry;
  }

  @Override
  public void process(ApplicationWithPrograms input) throws Exception {
    store.addApplication(input.getId(), input.getSpecification(), input.getLocation());

    // register usage
    ApplicationSpecification app = input.getSpecification();
    Id.Application appId = input.getId();
    Id.Namespace namespace = appId.getNamespace();

    for (FlowSpecification flow : app.getFlows().values()) {
      Id.Flow programId = Id.Flow.from(appId, flow.getName());
      for (FlowletConnection connection : flow.getConnections()) {
        if (connection.getSourceType().equals(FlowletConnection.Type.STREAM)) {
          usageRegistry.register(programId, Id.Stream.from(namespace, connection.getSourceName()));
        }
      }
      for (FlowletDefinition flowlet : flow.getFlowlets().values()) {
        for (String dataset : flowlet.getDatasets()) {
          usageRegistry.register(programId, Id.DatasetInstance.from(namespace, dataset));
        }
      }
    }

    for (MapReduceSpecification program : app.getMapReduce().values()) {
      Id.Program programId = Id.Program.from(appId, ProgramType.MAPREDUCE, program.getName());
      for (String dataset : program.getDataSets()) {
        if (!dataset.startsWith(Constants.Stream.URL_PREFIX)) {
          usageRegistry.register(programId, Id.DatasetInstance.from(namespace, dataset));
        }
      }
      String inputDatasetName = program.getInputDataSet();
      if (inputDatasetName != null && inputDatasetName.startsWith(Constants.Stream.URL_PREFIX)) {
        StreamBatchReadable stream = new StreamBatchReadable(URI.create(inputDatasetName));
        usageRegistry.register(programId, Id.Stream.from(namespace, stream.getStreamName()));
      }
    }

    emit(input);
  }
}
