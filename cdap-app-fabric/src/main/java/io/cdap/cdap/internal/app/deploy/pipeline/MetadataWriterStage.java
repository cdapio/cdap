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
import io.cdap.cdap.api.artifact.ApplicationClass;
import io.cdap.cdap.api.metadata.Metadata;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.data2.metadata.system.AppSystemMetadataWriter;
import io.cdap.cdap.data2.metadata.system.ProgramSystemMetadataWriter;
import io.cdap.cdap.data2.metadata.writer.MetadataServiceClient;
import io.cdap.cdap.pipeline.AbstractStage;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.spi.metadata.MetadataMutation;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Stage to write metadata for an application.
 */
public class MetadataWriterStage extends AbstractStage<ApplicationWithPrograms> {

  private final MetadataServiceClient metadataServiceClient;

  public MetadataWriterStage(MetadataServiceClient metadataServiceClient) {
    super(TypeToken.of(ApplicationWithPrograms.class));
    this.metadataServiceClient = metadataServiceClient;
  }

  @Override
  public void process(ApplicationWithPrograms input) {
    // use current time as creation time for app and all programs
    String creationTime = String.valueOf(System.currentTimeMillis());

    ApplicationId appId = input.getApplicationId();
    // get the system metadata and combine it with our own system metadata, metadata creation mutation will
    // completely override the previous metadata in the scope, so we have to combine these together
    Metadata systemAppMetadata = input.getMetadata().get(MetadataScope.SYSTEM);
    List<MetadataMutation> mutations = new ArrayList<>(createSystemMetadataMutations(
        metadataServiceClient, appId, input.getSpecification(), input.getApplicationClass(),
        creationTime, systemAppMetadata));

    // add the rest user defined metadata
    Metadata userAppMetadata = input.getMetadata().get(MetadataScope.USER);
    if (userAppMetadata != null
        && (!userAppMetadata.getProperties().isEmpty() || !userAppMetadata.getTags().isEmpty())) {
      mutations.add(new MetadataMutation.Create(
          appId.toMetadataEntity(),
          new io.cdap.cdap.spi.metadata.Metadata(MetadataScope.USER, userAppMetadata.getTags(),
              userAppMetadata.getProperties()), Collections.emptyMap()));
    }

    // write all metadata
    metadataServiceClient.batch(mutations);

    // Emit input to the next stage
    emit(input);
  }

  /**
   * Returns the mutations that create the system metadata of an application and of its programs.
   *
   * @param metadataServiceClient the client that the metadata writers require
   * @param appId the application
   * @param appSpec the specification of the application
   * @param applicationClass the application class in the artifact of the application
   * @param creationTime the creation time of the application and its programs, in milliseconds
   * @param systemAppMetadata system metadata that the application emitted, or null if none
   */
  public static List<MetadataMutation.Create> createSystemMetadataMutations(
      MetadataServiceClient metadataServiceClient, ApplicationId appId,
      ApplicationSpecification appSpec, ApplicationClass applicationClass, String creationTime,
      @Nullable Metadata systemAppMetadata) {
    List<MetadataMutation.Create> mutations = new ArrayList<>();
    // add system metadata for apps
    mutations.add(
        new AppSystemMetadataWriter(metadataServiceClient, appId, appSpec, applicationClass,
            creationTime, systemAppMetadata)
            .getMetadataMutation());

    // collect system metadata for programs
    collectProgramSystemMetadata(metadataServiceClient, appId, ProgramType.MAPREDUCE,
        appSpec.getMapReduce().values(), creationTime, mutations);
    collectProgramSystemMetadata(metadataServiceClient, appId, ProgramType.SERVICE,
        appSpec.getServices().values(), creationTime, mutations);
    collectProgramSystemMetadata(metadataServiceClient, appId, ProgramType.SPARK,
        appSpec.getSpark().values(), creationTime, mutations);
    collectProgramSystemMetadata(metadataServiceClient, appId, ProgramType.WORKER,
        appSpec.getWorkers().values(), creationTime, mutations);
    collectProgramSystemMetadata(metadataServiceClient, appId, ProgramType.WORKFLOW,
        appSpec.getWorkflows().values(), creationTime, mutations);
    return mutations;
  }

  private static void collectProgramSystemMetadata(MetadataServiceClient metadataServiceClient,
      ApplicationId appId, ProgramType programType,
      Iterable<? extends ProgramSpecification> specs, String creationTime,
      List<MetadataMutation.Create> mutations) {
    for (ProgramSpecification spec : specs) {
      ProgramId programId = appId.program(programType, spec.getName());
      mutations.add(
          new ProgramSystemMetadataWriter(metadataServiceClient, programId, spec,
              creationTime).getMetadataMutation());
    }
  }
}
