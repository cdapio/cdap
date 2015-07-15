/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
package co.cask.cdap.internal.app.runtime.distributed;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.api.workflow.ScheduleProgramInfo;
import co.cask.cdap.api.workflow.Workflow;
import co.cask.cdap.api.workflow.WorkflowActionNode;
import co.cask.cdap.api.workflow.WorkflowConditionNode;
import co.cask.cdap.api.workflow.WorkflowForkNode;
import co.cask.cdap.api.workflow.WorkflowNode;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.proto.ProgramType;
import com.google.common.collect.Iterables;
import org.apache.twill.api.EventHandler;
import org.apache.twill.api.TwillApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import javax.annotation.Nullable;

/**
 * The {@link TwillApplication} for running {@link Workflow} in distributed mode.
 */
public class WorkflowTwillApplication extends AbstractProgramTwillApplication {

  private static final Logger LOG = LoggerFactory.getLogger(WorkflowTwillApplication.class);
  private static final int WORKFLOW_MEMORY_MB = 512;

  private final ApplicationSpecification appSpec;
  private final WorkflowSpecification spec;

  public WorkflowTwillApplication(Program program, WorkflowSpecification spec,
                                  Map<String, File> localizeFiles,
                                  EventHandler eventHandler) {
    super(program, localizeFiles, eventHandler);
    this.appSpec = program.getApplicationSpecification();
    this.spec = spec;
  }

  @Override
  protected ProgramType getType() {
    return ProgramType.WORKFLOW;
  }

  /**
   * Returns the Spark driver resources requirement or null if the node doesn't represents a Spark program or
   * the driver resources is not set.
   */
  @Nullable
  private Resources getSparkDriverResources(WorkflowActionNode actionNode) {
    ScheduleProgramInfo programInfo = actionNode.getProgram();
    if (programInfo.getProgramType() != SchedulableProgramType.SPARK) {
      return null;
    }
    SparkSpecification sparkSpec = appSpec.getSpark().get(programInfo.getProgramName());

    // The sparkSpec shouldn't be null, otherwise the Workflow is not valid
    return sparkSpec.getDriverResources();
  }

  @Override
  protected void addRunnables(Map<String, RunnableResource> runnables) {
    // Find the resource requirements from the workflow
    // It is the largest memory and cores from all Spark program inside the workflow
    int memoryMB = WORKFLOW_MEMORY_MB;
    int vcores = 1;

    // Search through all workflow nodes for spark program resource requirements.
    Queue<WorkflowNode> nodes = new LinkedList<>(spec.getNodes());
    while (!nodes.isEmpty()) {
      WorkflowNode node = nodes.poll();
      switch (node.getType()) {
        case ACTION: {
          Resources driverResources = getSparkDriverResources((WorkflowActionNode) node);
          if (driverResources != null) {
            memoryMB = Math.max(memoryMB, driverResources.getMemoryMB());
            vcores = Math.max(vcores, driverResources.getVirtualCores());
          }
          break;
        }
        case FORK: {
          WorkflowForkNode forkNode = (WorkflowForkNode) node;
          Iterables.addAll(nodes, Iterables.concat(forkNode.getBranches()));
          break;
        }
        case CONDITION: {
          WorkflowConditionNode conditionNode = (WorkflowConditionNode) node;
          nodes.addAll(conditionNode.getIfBranch());
          nodes.addAll(conditionNode.getElseBranch());
          break;
        }
        default:
          LOG.warn("Unknown workflow node type {}", node.getType());
      }
    }

    runnables.put(spec.getName(), new RunnableResource(
      new WorkflowTwillRunnable(spec.getName(), "hConf.xml", "cConf.xml"),
      createResourceSpec(new Resources(memoryMB, vcores), 1)
    ));
  }
}
