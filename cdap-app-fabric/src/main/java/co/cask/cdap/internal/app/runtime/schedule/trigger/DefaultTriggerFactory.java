/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.schedule.trigger;

import co.cask.cdap.api.ProgramStatus;
import co.cask.cdap.api.app.ProgramType;
import co.cask.cdap.api.schedule.Trigger;
import co.cask.cdap.api.schedule.TriggerFactory;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;

import java.util.Map;

/**
 * The default implementation of {@link TriggerFactory}
 */
public class DefaultTriggerFactory implements TriggerFactory {

  private final NamespaceId namespaceId;

  public DefaultTriggerFactory(NamespaceId namespaceId) {
    this.namespaceId = namespaceId;
  }

  @Override
  public Trigger or(Trigger... triggers) {
    return new OrTriggerBuilder(triggers);
  }

  @Override
  public Trigger and(Trigger... triggers) {
    return new AndTriggerBuilder(triggers);
  }

  @Override
  public Trigger byTime(String cronExpression) {
    return new TimeTrigger(cronExpression);
  }

  @Override
  public Trigger onPartitions(String datasetName, int numPartitions) {
    return new PartitionTrigger(namespaceId.dataset(datasetName), numPartitions);
  }

  @Override
  public Trigger onPartitions(String datasetNamespace, String datasetName, int numPartitions) {
    return new PartitionTrigger(new DatasetId(datasetNamespace, datasetName), numPartitions);
  }

  @Override
  public Trigger onProgramStatus(String programNamespace, String application, String appVersion,
                                 ProgramType programType, String program, ProgramStatus... programStatuses) {
    return new ProgramStatusTrigger(new ApplicationId(programNamespace, application, appVersion)
                                      .program(co.cask.cdap.proto.ProgramType.valueOf(programType.name()), program),
                                    programStatuses);
  }

  @Override
  public Trigger onProgramStatus(String programNamespace, String application, String appVersion,
                                 ProgramType programType, String program, Map<String, String> runtimeArgs,
                                 ProgramStatus... programStatuses) {
     return new ProgramStatusTrigger(new ApplicationId(programNamespace, application, appVersion)
                                       .program(co.cask.cdap.proto.ProgramType.valueOf(programType.name()), program),
                                     runtimeArgs, programStatuses);
  }

  @Override
  public Trigger onProgramStatus(String programNamespace, String application, ProgramType programType,
                                 String program, ProgramStatus... programStatuses) {
    return new ProgramStatusTrigger(new ApplicationId(programNamespace, application)
                                      .program(co.cask.cdap.proto.ProgramType.valueOf(programType.name()), program),
                                    programStatuses);
  }

  @Override
  public Trigger onProgramStatus(String application, ProgramType programType, String program,
                                 ProgramStatus... programStatuses) {
    return new ProgramStatusTrigger(new ProgramId(namespaceId.getNamespace(), application,
                                                  co.cask.cdap.proto.ProgramType.valueOf(programType.name()), program),
                                    programStatuses);
  }

  @Override
  public Trigger onProgramStatus(ProgramType programType, String program, ProgramStatus... programStatuses) {
    return new ProgramStatusTriggerBuilder(programType.name(), program, programStatuses);
  }

  @Override
  public Trigger onProgramStatus(ProgramType programType, String program, Map<String, String> runtimeArgs,
                                 ProgramStatus... programStatuses) {
    return new ProgramStatusTriggerBuilder(programType.name(), program, runtimeArgs, programStatuses);
  }
}
