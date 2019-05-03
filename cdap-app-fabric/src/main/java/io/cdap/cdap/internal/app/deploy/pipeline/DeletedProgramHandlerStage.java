/*
 * Copyright © 2014-2019 Cask Data, Inc.
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
import io.cdap.cdap.api.metrics.MetricDeleteQuery;
import io.cdap.cdap.api.metrics.MetricsSystemClient;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.data2.metadata.writer.MetadataServiceClient;
import io.cdap.cdap.internal.app.deploy.ProgramTerminator;
import io.cdap.cdap.pipeline.AbstractStage;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.ProgramTypes;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.scheduler.Scheduler;
import io.cdap.cdap.spi.metadata.MetadataMutation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Deleted program handler stage. Figures out which programs are deleted and handles callback.
 */
public class DeletedProgramHandlerStage extends AbstractStage<ApplicationDeployable> {

  private static final Logger LOG = LoggerFactory.getLogger(DeletedProgramHandlerStage.class);

  private final Store store;
  private final ProgramTerminator programTerminator;
  private final MetricsSystemClient metricsSystemClient;
  private final MetadataServiceClient metadataServiceClient;
  private final Scheduler programScheduler;

  public DeletedProgramHandlerStage(Store store, ProgramTerminator programTerminator,
                                    MetricsSystemClient metricsSystemClient,
                                    MetadataServiceClient metadataServiceClient,
                                    Scheduler programScheduler) {
    super(TypeToken.of(ApplicationDeployable.class));
    this.store = store;
    this.programTerminator = programTerminator;
    this.metricsSystemClient = metricsSystemClient;
    this.metadataServiceClient = metadataServiceClient;
    this.programScheduler = programScheduler;
  }

  @Override
  public void process(ApplicationDeployable appSpec) throws Exception {
    List<ProgramSpecification> deletedSpecs = store.getDeletedProgramSpecifications(appSpec.getApplicationId(),
                                                                                    appSpec.getSpecification());

    // TODO: this should also delete logs and run records (or not?), and do it for all program types [CDAP-2187]

    Set<ProgramId> deletedPrograms = new HashSet<>();
    for (ProgramSpecification spec : deletedSpecs) {
      //call the deleted spec
      ProgramType type = ProgramTypes.fromSpecification(spec);
      ProgramId programId = appSpec.getApplicationId().program(type, spec.getName());
      programTerminator.stop(programId);
      programScheduler.deleteSchedules(programId);
      programScheduler.modifySchedulesTriggeredByDeletedProgram(programId);

      // Remove metadata for the deleted program
      metadataServiceClient.drop(new MetadataMutation.Drop(programId.toMetadataEntity()));

      deletedPrograms.add(programId);
    }

    deleteMetrics(deletedPrograms);

    emit(appSpec);
  }

  private void deleteMetrics(Set<ProgramId> programs) throws IOException {
    for (ProgramId programId : programs) {
      LOG.debug("Deleting metrics for program {}", programId);

      String typeTag = getMetricsTag(programId.getType());

      if (typeTag != null) {
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put(Constants.Metrics.Tag.NAMESPACE, programId.getNamespace());
        tags.put(Constants.Metrics.Tag.APP, programId.getApplication());
        tags.put(typeTag, programId.getProgram());

        long endTs = System.currentTimeMillis() / 1000;
        MetricDeleteQuery deleteQuery = new MetricDeleteQuery(0, endTs, Collections.emptySet(), tags,
                                                              new ArrayList<>(tags.keySet()));
        metricsSystemClient.delete(deleteQuery);
      }
    }
  }

  @Nullable
  private String getMetricsTag(ProgramType type) {
    switch (type) {
      case MAPREDUCE:
        return Constants.Metrics.Tag.MAPREDUCE;
      case WORKFLOW:
        return Constants.Metrics.Tag.WORKFLOW;
      case SERVICE:
        return Constants.Metrics.Tag.SERVICE;
      case SPARK:
        return Constants.Metrics.Tag.SPARK;
      case WORKER:
        return Constants.Metrics.Tag.WORKER;
      default:
        return null;
    }
  }
}
