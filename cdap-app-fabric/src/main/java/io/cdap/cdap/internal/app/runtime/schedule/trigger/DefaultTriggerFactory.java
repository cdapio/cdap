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

package io.cdap.cdap.internal.app.runtime.schedule.trigger;

import io.cdap.cdap.api.ProgramStatus;
import io.cdap.cdap.api.app.ProgramType;
import io.cdap.cdap.api.schedule.Trigger;
import io.cdap.cdap.api.schedule.TriggerFactory;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;

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
  public Trigger onProgramStatus(String namespace, String application, String appVersion,
                                 ProgramType programType, String program, ProgramStatus... programStatuses) {
    return new ProgramStatusTrigger(new ApplicationId(namespace, application, appVersion)
                                      .program(io.cdap.cdap.proto.ProgramType.valueOf(programType.name()), program),
                                    programStatuses);
  }

  @Override
  public Trigger onProgramStatus(String programNamespace, String application, ProgramType programType,
                                 String program, ProgramStatus... programStatuses) {
    return new ProgramStatusTrigger(new ApplicationId(programNamespace, application)
                                      .program(io.cdap.cdap.proto.ProgramType.valueOf(programType.name()), program),
                                    programStatuses);
  }

  @Override
  public Trigger onProgramStatus(String application, ProgramType programType, String program,
                                 ProgramStatus... programStatuses) {
    return new ProgramStatusTrigger(new ProgramId(namespaceId.getNamespace(), application,
                                                  io.cdap.cdap.proto.ProgramType.valueOf(programType.name()), program),
                                    programStatuses);
  }

  @Override
  public Trigger onProgramStatus(ProgramType programType, String program, ProgramStatus... programStatuses) {
    return new ProgramStatusTriggerBuilder(programType.name(), program, programStatuses);
  }
}
