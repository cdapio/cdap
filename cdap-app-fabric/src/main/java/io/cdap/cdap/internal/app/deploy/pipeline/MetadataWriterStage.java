/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.deploy.pipeline;

import com.google.common.reflect.TypeToken;
import io.cdap.cdap.api.ProgramSpecification;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.data2.metadata.system.AppSystemMetadataWriter;
import io.cdap.cdap.data2.metadata.system.ProgramSystemMetadataWriter;
import io.cdap.cdap.data2.metadata.writer.MetadataServiceClient;
import io.cdap.cdap.pipeline.AbstractStage;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.spi.metadata.MetadataMutation;

import java.util.ArrayList;
import java.util.List;

/**
 * Stage to write metadata for an application.
 */
public class MetadataWriterStage extends AbstractStage<ApplicationWithPrograms> {

  private final MetadataServiceClient metadataServiceClient;
  private String creationTime;

  public MetadataWriterStage(MetadataServiceClient metadataServiceClient) {
    super(TypeToken.of(ApplicationWithPrograms.class));
    this.metadataServiceClient = metadataServiceClient;
  }

  @Override
  public void process(ApplicationWithPrograms input) {
    // use current time as creation time for app and all programs
    creationTime = String.valueOf(System.currentTimeMillis());

    // add system metadata for apps
    ApplicationId appId = input.getApplicationId();
    ApplicationSpecification appSpec = input.getSpecification();
    List<MetadataMutation> mutations = new ArrayList<>();

    mutations.add(
      new AppSystemMetadataWriter(metadataServiceClient, appId, appSpec, input.getApplicationClass(), creationTime)
        .getMetadataMutation());

    // collect system metadata for programs
    collectProgramSystemMetadata(appId, ProgramType.MAPREDUCE, appSpec.getMapReduce().values(), mutations);
    collectProgramSystemMetadata(appId, ProgramType.SERVICE, appSpec.getServices().values(), mutations);
    collectProgramSystemMetadata(appId, ProgramType.SPARK, appSpec.getSpark().values(), mutations);
    collectProgramSystemMetadata(appId, ProgramType.WORKER, appSpec.getWorkers().values(), mutations);
    collectProgramSystemMetadata(appId, ProgramType.WORKFLOW, appSpec.getWorkflows().values(), mutations);

    // add the rest user defined metadata
    if (input.getMetadata() != null) {
      mutations.add(input.getMetadata());
    }

    // write all metadata
    metadataServiceClient.batch(mutations);

    // Emit input to the next stage
    emit(input);
  }

  private void collectProgramSystemMetadata(ApplicationId appId, ProgramType programType,
                                            Iterable<? extends ProgramSpecification> specs,
                                            List<MetadataMutation> mutations) {
    for (ProgramSpecification spec : specs) {
      ProgramId programId = appId.program(programType, spec.getName());
      mutations.add(
        new ProgramSystemMetadataWriter(metadataServiceClient, programId, spec, creationTime).getMetadataMutation());
    }
  }
}
