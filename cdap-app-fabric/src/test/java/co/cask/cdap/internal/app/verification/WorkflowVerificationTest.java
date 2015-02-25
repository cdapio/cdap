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

package co.cask.cdap.internal.app.verification;

import co.cask.cdap.GoodWorkflowApp;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.workflow.ScheduleProgramInfo;
import co.cask.cdap.api.workflow.WorkflowActionNode;
import co.cask.cdap.api.workflow.WorkflowForkNode;
import co.cask.cdap.api.workflow.WorkflowNode;
import co.cask.cdap.api.workflow.WorkflowNodeType;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.internal.app.Specifications;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 *
 */
public class WorkflowVerificationTest {
  @Test
  public void testGoodWorkflow() throws Exception {
    ApplicationSpecification appSpec = Specifications.from(new GoodWorkflowApp());
    Assert.assertTrue(appSpec.getWorkflows().size() == 1);
    WorkflowSpecification spec = appSpec.getWorkflows().get("GoodWorkflow");
    Assert.assertTrue(spec.getNodes().size() == 4);
    List<WorkflowNode> nodes = spec.getNodes();

    WorkflowNode node = nodes.get(0);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    WorkflowActionNode actionNode = (WorkflowActionNode) node;
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.CUSTOM_ACTION,
                                                                             "DummyAction")));

    node = nodes.get(1);
    Assert.assertTrue(node.getType() == WorkflowNodeType.FORK);

    WorkflowForkNode fork1 = (WorkflowForkNode) node;
    Assert.assertTrue(fork1.getBranches().size() == 2);

    List<WorkflowNode> fork1Branch1 = fork1.getBranches().get(0);
    Assert.assertTrue(fork1Branch1.size() == 2);
    List<WorkflowNode> fork1Branch2 = fork1.getBranches().get(1);
    Assert.assertTrue(fork1Branch2.size() == 1);


    actionNode = (WorkflowActionNode) fork1Branch1.get(0);
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "DummyMR")));

    WorkflowForkNode fork2 = (WorkflowForkNode) fork1Branch1.get(1);
    List<WorkflowNode> fork2Branch1 = fork2.getBranches().get(0);
    Assert.assertTrue(fork2Branch1.size() == 2);
    List<WorkflowNode> fork2Branch2 = fork2.getBranches().get(1);
    Assert.assertTrue(fork2Branch2.size() == 1);

    actionNode = (WorkflowActionNode) fork2Branch1.get(0);
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.CUSTOM_ACTION,
                                                                             "DummyAction")));


    WorkflowForkNode fork3 = (WorkflowForkNode) fork2Branch1.get(1);
    List<WorkflowNode> fork3Branch1 = fork3.getBranches().get(0);
    Assert.assertTrue(fork3Branch1.size() == 2);
    List<WorkflowNode> fork3Branch2 = fork3.getBranches().get(1);
    Assert.assertTrue(fork3Branch2.size() == 1);

    WorkflowForkNode fork4 = (WorkflowForkNode) fork3Branch1.get(0);
    List<WorkflowNode> fork4Branch1 = fork4.getBranches().get(0);
    Assert.assertTrue(fork4Branch1.size() == 2);
    List<WorkflowNode> fork4Branch2 = fork4.getBranches().get(1);
    Assert.assertTrue(fork4Branch2.size() == 1);

    actionNode = (WorkflowActionNode) fork4Branch1.get(0);
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "DummyMR")));
    actionNode = (WorkflowActionNode) fork4Branch1.get(1);
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.CUSTOM_ACTION,
                                                                             "DummyAction")));

    actionNode = (WorkflowActionNode) fork4Branch2.get(0);
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "DummyMR")));

    actionNode = (WorkflowActionNode) fork4Branch1.get(0);
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "DummyMR")));

    actionNode = (WorkflowActionNode) fork4Branch2.get(0);
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "DummyMR")));

    actionNode = (WorkflowActionNode) fork2Branch2.get(0);
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.CUSTOM_ACTION,
                                                                             "DummyAction")));

    actionNode = (WorkflowActionNode) fork1Branch2.get(0);
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.CUSTOM_ACTION,
                                                                             "DummyAction")));

    node = nodes.get(2);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    actionNode = (WorkflowActionNode) node;
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "DummyMR")));

    node = nodes.get(3);
    WorkflowForkNode fork5 = (WorkflowForkNode) node;
    List<WorkflowNode> fork5Branch1 = fork5.getBranches().get(0);
    Assert.assertTrue(fork5Branch1.size() == 1);
    List<WorkflowNode> fork5Branch2 = fork5.getBranches().get(1);
    Assert.assertTrue(fork5Branch2.size() == 1);

    actionNode = (WorkflowActionNode) fork5Branch1.get(0);
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.CUSTOM_ACTION,
                                                                             "DummyAction")));

    actionNode = (WorkflowActionNode) fork5Branch2.get(0);
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "DummyMR")));
  }
}
