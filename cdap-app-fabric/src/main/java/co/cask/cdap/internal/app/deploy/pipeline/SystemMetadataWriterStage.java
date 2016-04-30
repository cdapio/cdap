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

package co.cask.cdap.internal.app.deploy.pipeline;

import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.data2.metadata.system.AppSystemMetadataWriter;
import co.cask.cdap.data2.metadata.system.ProgramSystemMetadataWriter;
import co.cask.cdap.data2.metadata.system.SystemMetadataWriter;
import co.cask.cdap.pipeline.AbstractStage;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.reflect.TypeToken;

/**
 * Stage to write system metadata for an application.
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
    SystemMetadataWriter appSystemMetadataWriter = new AppSystemMetadataWriter(metadataStore, input.getId(),
                                                                                       appSpec);
    appSystemMetadataWriter.write();

    // add system metadata for programs
    writeProgramSystemMetadata(input.getId(), ProgramType.FLOW, appSpec.getFlows().values());
    writeProgramSystemMetadata(input.getId(), ProgramType.MAPREDUCE, appSpec.getMapReduce().values());
    writeProgramSystemMetadata(input.getId(), ProgramType.SERVICE, appSpec.getServices().values());
    writeProgramSystemMetadata(input.getId(), ProgramType.SPARK, appSpec.getSpark().values());
    writeProgramSystemMetadata(input.getId(), ProgramType.WORKER, appSpec.getWorkers().values());
    writeProgramSystemMetadata(input.getId(), ProgramType.WORKFLOW, appSpec.getWorkflows().values());

    // Emit input to the next stage
    emit(input);
  }

  private void writeProgramSystemMetadata(Id.Application appId, ProgramType programType,
                                          Iterable<? extends ProgramSpecification> specs) {
    for (ProgramSpecification spec : specs) {
      ProgramId programId = appId.toEntityId().program(programType, spec.getName());
      ProgramSystemMetadataWriter writer = new ProgramSystemMetadataWriter(metadataStore, programId, spec);
      writer.write();
    }
  }
}
