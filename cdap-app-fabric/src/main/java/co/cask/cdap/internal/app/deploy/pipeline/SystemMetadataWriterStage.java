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

package co.cask.cdap.internal.app.deploy.pipeline;

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.api.worker.WorkerSpecification;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.data2.metadata.system.AbstractSystemMetadataWriter;
import co.cask.cdap.data2.metadata.system.AppSystemMetadataWriter;
import co.cask.cdap.data2.metadata.system.ProgramSystemMetadataWriter;
import co.cask.cdap.pipeline.AbstractStage;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import com.google.common.reflect.TypeToken;

import java.util.ArrayList;
import java.util.List;

/**
 * Stage to write system metadata for the application.
 */
public class SystemMetadataWriterStage extends AbstractStage<ApplicationWithPrograms> {

  private final MetadataStore metadataStore;

  public SystemMetadataWriterStage(MetadataStore metadataStore) {
    super(TypeToken.of(ApplicationWithPrograms.class));
    this.metadataStore = metadataStore;
  }

  @Override
  public void process(ApplicationWithPrograms input) throws Exception {
    // add system metadata for apps
    ApplicationSpecification appSpec = input.getSpecification();
    AbstractSystemMetadataWriter appSystemMetadataWriter = new AppSystemMetadataWriter(metadataStore, input.getId(),
                                                                                       appSpec);
    appSystemMetadataWriter.write();

    // add system metadata for programs
    Id.Program programId;
    List<AbstractSystemMetadataWriter> programSystemMetadataWriters = new ArrayList<>();
    for (MapReduceSpecification mrSpec : appSpec.getMapReduce().values()) {
      programId = Id.Program.from(input.getId(), ProgramType.MAPREDUCE, mrSpec.getName());
      programSystemMetadataWriters.add(new ProgramSystemMetadataWriter(metadataStore, programId, mrSpec));
    }
    for (FlowSpecification flowSpec : appSpec.getFlows().values()) {
      programId = Id.Program.from(input.getId(), ProgramType.FLOW, flowSpec.getName());
      programSystemMetadataWriters.add(new ProgramSystemMetadataWriter(metadataStore, programId, flowSpec));
    }
    for (WorkflowSpecification workflowSpec : appSpec.getWorkflows().values()) {
      programId = Id.Program.from(input.getId(), ProgramType.WORKFLOW, workflowSpec.getName());
      programSystemMetadataWriters.add(new ProgramSystemMetadataWriter(metadataStore, programId, workflowSpec));
    }
    for (ServiceSpecification serviceSpec : appSpec.getServices().values()) {
      programId = Id.Program.from(input.getId(), ProgramType.SERVICE, serviceSpec.getName());
      programSystemMetadataWriters.add(new ProgramSystemMetadataWriter(metadataStore, programId, serviceSpec));
    }
    for (SparkSpecification sparkSpec : appSpec.getSpark().values()) {
      programId = Id.Program.from(input.getId(), ProgramType.SPARK, sparkSpec.getName());
      programSystemMetadataWriters.add(new ProgramSystemMetadataWriter(metadataStore, programId, sparkSpec));
    }
    for (WorkerSpecification workerSpec : appSpec.getWorkers().values()) {
      programId = Id.Program.from(input.getId(), ProgramType.WORKER, workerSpec.getName());
      programSystemMetadataWriters.add(new ProgramSystemMetadataWriter(metadataStore, programId, workerSpec));
    }

    for (AbstractSystemMetadataWriter programSystemMetadataWriter : programSystemMetadataWriters) {
      programSystemMetadataWriter.write();
    }

    // Emit input to the next stage
    emit(input);
  }
}
