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

package co.cask.cdap.proto;

import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.api.procedure.ProcedureSpecification;
import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.api.webapp.WebappSpecification;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Provides mapping from {@link co.cask.cdap.api.ProgramSpecification} to {@link ProgramType}.
 */
public class ProgramTypes {

  private static final Map<Class<? extends ProgramSpecification>, ProgramType> specClassToProgramType =
    ImmutableMap.<Class<? extends ProgramSpecification>, ProgramType>builder()
      .put(FlowSpecification.class, ProgramType.FLOW)
      .put(ProcedureSpecification.class, ProgramType.PROCEDURE)
      .put(MapReduceSpecification.class, ProgramType.MAPREDUCE)
      .put(SparkSpecification.class, ProgramType.SPARK)
      .put(WorkflowSpecification.class, ProgramType.WORKFLOW)
      .put(WebappSpecification.class, ProgramType.WEBAPP)
      .put(ServiceSpecification.class, ProgramType.SERVICE)
      .build();

  /**
   * Maps from {@link ProgramSpecification} to {@link ProgramType}.
   *
   * @param spec {@link ProgramSpecification} to convert
   * @return {@link ProgramType} of the {@link ProgramSpecification}
   */
  public static ProgramType fromSpecification(ProgramSpecification spec) {
    Class<? extends ProgramSpecification> specClass = spec.getClass();
    for (Map.Entry<Class<? extends ProgramSpecification>, ProgramType> entry : specClassToProgramType.entrySet()) {
      if (entry.getKey().isAssignableFrom(specClass)) {
        return entry.getValue();
      }
    }
    throw new IllegalArgumentException("Unknown specification type: " + specClass);
  }

}
