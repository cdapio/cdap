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

import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;
import io.cdap.cdap.api.ProgramSpecification;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.pipeline.AbstractStage;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.ProgramTypes;
import io.cdap.cdap.proto.id.ProgramId;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class ProgramGenerationStage extends AbstractStage<ApplicationDeployable> {

  public ProgramGenerationStage() {
    super(TypeToken.of(ApplicationDeployable.class));
  }

  @Override
  public void process(final ApplicationDeployable input) throws Exception {
    List<ProgramDescriptor> programDescriptors = new ArrayList<>();
    final ApplicationSpecification appSpec = input.getSpecification();

    // Now, we iterate through all ProgramSpecification and generate programs
    Iterable<ProgramSpecification> specifications = Iterables.concat(
      appSpec.getMapReduce().values(),
      appSpec.getWorkflows().values(),
      appSpec.getServices().values(),
      appSpec.getSpark().values(),
      appSpec.getWorkers().values()
    );

    for (ProgramSpecification spec: specifications) {
      ProgramType type = ProgramTypes.fromSpecification(spec);
      ProgramId programId = input.getApplicationId().program(type, spec.getName());
      programDescriptors.add(new ProgramDescriptor(programId, appSpec));
    }

    emit(new ApplicationWithPrograms(input, programDescriptors));
  }
}
