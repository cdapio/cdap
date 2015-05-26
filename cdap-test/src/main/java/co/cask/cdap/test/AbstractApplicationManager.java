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

package co.cask.cdap.test;

import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * A base implementation of {@link ApplicationManager}.
 */
public abstract class AbstractApplicationManager implements ApplicationManager {

  @Override
  public FlowManager startFlow(final String flowName) {
    return startFlow(flowName, ImmutableMap.<String, String>of());
  }

  @Override
  public FlowManager startFlow(final String flowName, Map<String, String> arguments) {
    startProgram(flowName, arguments, ProgramType.FLOW);
    return getFlowManager(flowName);
  }

  @Override
  public MapReduceManager startMapReduce(final String programName) {
    return startMapReduce(programName, ImmutableMap.<String, String>of());
  }

  @Override
  public MapReduceManager startMapReduce(final String programName, Map<String, String> arguments) {
    startProgram(programName, arguments, ProgramType.MAPREDUCE);
    return getMapReduceManager(programName);
  }

  @Override
  public SparkManager startSpark(String programName) {
    return startSpark(programName, ImmutableMap.<String, String>of());
  }

  @Override
  public SparkManager startSpark(String programName, Map<String, String> arguments) {
    startProgram(programName, arguments, ProgramType.SPARK);
    return getSparkManager(programName);
  }

  @Override
  public WorkflowManager startWorkflow(String workflowName) {
    return startWorkflow(workflowName, ImmutableMap.<String, String>of());
  }

  @Override
  public WorkflowManager startWorkflow(final String workflowName, Map<String, String> arguments) {
    startProgram(workflowName, arguments, ProgramType.WORKFLOW);
    return getWorkflowManager(workflowName);
  }

  @Override
  public ServiceManager startService(String serviceName) {
    return startService(serviceName, ImmutableMap.<String, String>of());
  }

  @Override
  public ServiceManager startService(final String serviceName, Map<String, String> arguments) {
    startProgram(serviceName, arguments, ProgramType.SERVICE);
    return getServiceManager(serviceName);
  }

  @Override
  public WorkerManager startWorker(String workerName) {
    return startWorker(workerName, ImmutableMap.<String, String>of());
  }

  @Override
  public WorkerManager startWorker(final String workerName, Map<String, String> arguments) {
    startProgram(workerName, arguments, ProgramType.WORKER);
    return getWorkerManager(workerName);
  }

  protected abstract Id.Program startProgram(String programName, Map<String, String> arguments,
                                             ProgramType programType);
}
