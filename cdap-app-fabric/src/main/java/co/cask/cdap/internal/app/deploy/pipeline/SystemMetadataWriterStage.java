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
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.metadata.MetadataScope;
import com.google.common.reflect.TypeToken;

import java.util.Map;

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
    ApplicationId appId = input.getApplicationId();
    ApplicationSpecification appSpec = input.getSpecification();
    // only update creation time if this is a new app
    Map<String, String> properties = metadataStore.getProperties(MetadataScope.SYSTEM, appId);
    SystemMetadataWriter appSystemMetadataWriter =
      new AppSystemMetadataWriter(metadataStore, appId, appSpec, !properties.isEmpty());
    appSystemMetadataWriter.write();

    // add system metadata for programs
    writeProgramSystemMetadata(appId, ProgramType.FLOW, appSpec.getFlows().values());
    writeProgramSystemMetadata(appId, ProgramType.MAPREDUCE, appSpec.getMapReduce().values());
    writeProgramSystemMetadata(appId, ProgramType.SERVICE, appSpec.getServices().values());
    writeProgramSystemMetadata(appId, ProgramType.SPARK, appSpec.getSpark().values());
    writeProgramSystemMetadata(appId, ProgramType.WORKER, appSpec.getWorkers().values());
    writeProgramSystemMetadata(appId, ProgramType.WORKFLOW, appSpec.getWorkflows().values());

    // Emit input to the next stage
    emit(input);
  }

  private void writeProgramSystemMetadata(ApplicationId appId, ProgramType programType,
                                          Iterable<? extends ProgramSpecification> specs) {
    for (ProgramSpecification spec : specs) {
      ProgramId programId = appId.program(programType, spec.getName());
      Map<String, String> properties = metadataStore.getProperties(MetadataScope.SYSTEM, programId);
      ProgramSystemMetadataWriter writer = new ProgramSystemMetadataWriter(metadataStore, programId, spec,
                                                                           !properties.isEmpty());
      writer.write();
    }
  }
}
