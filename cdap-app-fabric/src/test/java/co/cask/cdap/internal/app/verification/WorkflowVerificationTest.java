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
import co.cask.cdap.api.workflow.WorkflowConditionNode;
import co.cask.cdap.api.workflow.WorkflowForkNode;
import co.cask.cdap.api.workflow.WorkflowNode;
import co.cask.cdap.api.workflow.WorkflowNodeType;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.Specifications;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
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
    verifyGoodWorkflowSpecifications(appSpec);
    verifyAnotherGoodWorkflowSpecification(appSpec);
    ApplicationSpecificationAdapter adapter = ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator());
    ApplicationSpecification newSpec = adapter.fromJson(adapter.toJson(appSpec));
    verifyGoodWorkflowSpecifications(newSpec);
    verifyAnotherGoodWorkflowSpecification(appSpec);
  }

  private void verifyAnotherGoodWorkflowSpecification(ApplicationSpecification appSpec) {
    WorkflowSpecification spec = appSpec.getWorkflows().get("AnotherGoodWorkflow");
    List<WorkflowNode> nodes = spec.getNodes();
    Assert.assertTrue(nodes.size() == 4);

    WorkflowNode node = nodes.get(0);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    Assert.assertEquals("MR1", node.getNodeId());
    WorkflowActionNode actionNode = (WorkflowActionNode) node;
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "MR1")));

    node = nodes.get(1);
    Assert.assertTrue(node.getType() == WorkflowNodeType.FORK);

    WorkflowForkNode fork = (WorkflowForkNode) node;
    Assert.assertTrue(fork.getBranches().size() == 2);
    List<WorkflowNode> forkBranch1 = fork.getBranches().get(0);
    Assert.assertTrue(forkBranch1.size() == 3);

    node = forkBranch1.get(0);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    Assert.assertEquals("MR2", node.getNodeId());
    actionNode = (WorkflowActionNode) node;
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "MR2")));
    node = forkBranch1.get(1);
    Assert.assertTrue(node.getType() == WorkflowNodeType.CONDITION);
    Assert.assertEquals("MyVerificationPredicate", node.getNodeId());
    WorkflowConditionNode condition = (WorkflowConditionNode) node;
    Assert.assertTrue(condition.getPredicateClassName().contains("MyVerificationPredicate"));
    List<WorkflowNode> ifNodes = condition.getIfBranch();
    Assert.assertTrue(ifNodes.size() == 2);
    List<WorkflowNode> elseNodes = condition.getElseBranch();
    Assert.assertTrue(elseNodes.size() == 2);

    node = ifNodes.get(0);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    Assert.assertEquals("MR3", node.getNodeId());
    actionNode = (WorkflowActionNode) node;
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "MR3")));

    node = ifNodes.get(1);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    Assert.assertEquals("MR4", node.getNodeId());
    actionNode = (WorkflowActionNode) node;
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "MR4")));

    node = elseNodes.get(0);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    Assert.assertEquals("MR5", node.getNodeId());
    actionNode = (WorkflowActionNode) node;
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "MR5")));

    node = elseNodes.get(1);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    Assert.assertEquals("MR6", node.getNodeId());
    actionNode = (WorkflowActionNode) node;
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "MR6")));
    node = forkBranch1.get(2);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    Assert.assertEquals("MR7", node.getNodeId());
    actionNode = (WorkflowActionNode) node;
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "MR7")));

    List<WorkflowNode> forkBranch2 = fork.getBranches().get(1);
    Assert.assertTrue(forkBranch2.size() == 1);

    node = forkBranch2.get(0);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    Assert.assertEquals("MR8", node.getNodeId());
    actionNode = (WorkflowActionNode) node;
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "MR8")));

    node = nodes.get(2);
    Assert.assertEquals("anotherMyVerificationPredicate", node.getNodeId());
    Assert.assertTrue(node.getType() == WorkflowNodeType.CONDITION);
    ifNodes = ((WorkflowConditionNode) node).getIfBranch();
    elseNodes = ((WorkflowConditionNode) node).getElseBranch();

    Assert.assertTrue(ifNodes.size() == 2);
    node = ifNodes.get(0);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    Assert.assertEquals("SP1", node.getNodeId());
    actionNode = (WorkflowActionNode) node;
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.SPARK,
                                                                             "SP1")));

    node = ifNodes.get(1);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    Assert.assertEquals("SP2", node.getNodeId());
    actionNode = (WorkflowActionNode) node;
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.SPARK,
                                                                             "SP2")));

    Assert.assertTrue(elseNodes.size() == 3);

    node = elseNodes.get(0);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    Assert.assertEquals("SP3", node.getNodeId());
    actionNode = (WorkflowActionNode) node;
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.SPARK,
                                                                             "SP3")));

    node = elseNodes.get(1);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    Assert.assertEquals("SP4", node.getNodeId());
    actionNode = (WorkflowActionNode) node;
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.SPARK,
                                                                             "SP4")));

    node = elseNodes.get(2);
    Assert.assertTrue(node.getType() == WorkflowNodeType.FORK);

    WorkflowForkNode anotherFork = (WorkflowForkNode) node;
    Assert.assertTrue(anotherFork.getBranches().size() == 2);
    List<WorkflowNode> anotherForkBranch1 = anotherFork.getBranches().get(0);
    Assert.assertTrue(anotherForkBranch1.size() == 1);
    List<WorkflowNode> anotherForkBranch2 = anotherFork.getBranches().get(1);
    Assert.assertTrue(anotherForkBranch2.size() == 1);

    node = anotherForkBranch1.get(0);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    Assert.assertEquals("SP5", node.getNodeId());
    actionNode = (WorkflowActionNode) node;
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.SPARK,
                                                                             "SP5")));

    node = anotherForkBranch2.get(0);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    Assert.assertEquals("SP6", node.getNodeId());
    actionNode = (WorkflowActionNode) node;
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.SPARK,
                                                                             "SP6")));

    node = nodes.get(3);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    Assert.assertEquals("SP7", node.getNodeId());
    actionNode = (WorkflowActionNode) node;
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.SPARK,
                                                                             "SP7")));
  }

  private void verifyGoodWorkflowSpecifications(ApplicationSpecification appSpec) {
    WorkflowSpecification spec = appSpec.getWorkflows().get("GoodWorkflow");
    Assert.assertTrue(spec.getNodes().size() == 4);
    List<WorkflowNode> nodes = spec.getNodes();

    WorkflowNode node = nodes.get(0);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    Assert.assertEquals("a1", node.getNodeId());
    WorkflowActionNode actionNode = (WorkflowActionNode) node;
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.CUSTOM_ACTION,
                                                                             "DummyAction")));
    Assert.assertNotNull(actionNode.getActionSpecification());

    node = nodes.get(1);
    Assert.assertTrue(node.getType() == WorkflowNodeType.FORK);

    WorkflowForkNode fork1 = (WorkflowForkNode) node;
    Assert.assertTrue(fork1.getBranches().size() == 2);

    List<WorkflowNode> fork1Branch1 = fork1.getBranches().get(0);
    Assert.assertTrue(fork1Branch1.size() == 2);
    List<WorkflowNode> fork1Branch2 = fork1.getBranches().get(1);
    Assert.assertTrue(fork1Branch2.size() == 1);

    actionNode = (WorkflowActionNode) fork1Branch1.get(0);
    Assert.assertEquals("mr1", actionNode.getNodeId());
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "DummyMR")));
    Assert.assertNull(actionNode.getActionSpecification());

    WorkflowForkNode fork2 = (WorkflowForkNode) fork1Branch1.get(1);
    List<WorkflowNode> fork2Branch1 = fork2.getBranches().get(0);
    Assert.assertTrue(fork2Branch1.size() == 2);
    List<WorkflowNode> fork2Branch2 = fork2.getBranches().get(1);
    Assert.assertTrue(fork2Branch2.size() == 1);

    actionNode = (WorkflowActionNode) fork2Branch1.get(0);
    Assert.assertEquals("a2", actionNode.getNodeId());
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.CUSTOM_ACTION,
                                                                             "DummyAction")));
    Assert.assertNotNull(actionNode.getActionSpecification());

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
    Assert.assertEquals("mr2", actionNode.getNodeId());
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "DummyMR")));
    Assert.assertNull(actionNode.getActionSpecification());

    actionNode = (WorkflowActionNode) fork4Branch1.get(1);
    Assert.assertEquals("a3", actionNode.getNodeId());
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.CUSTOM_ACTION,
                                                                             "DummyAction")));
    Assert.assertNotNull(actionNode.getActionSpecification());

    actionNode = (WorkflowActionNode) fork4Branch2.get(0);
    Assert.assertEquals("mr3", actionNode.getNodeId());
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "DummyMR")));
    Assert.assertNull(actionNode.getActionSpecification());

    actionNode = (WorkflowActionNode) fork3Branch1.get(1);
    Assert.assertEquals("mr4", actionNode.getNodeId());
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "DummyMR")));
    Assert.assertNull(actionNode.getActionSpecification());

    actionNode = (WorkflowActionNode) fork3Branch2.get(0);
    Assert.assertEquals("mr5", actionNode.getNodeId());
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "DummyMR")));
    Assert.assertNull(actionNode.getActionSpecification());

    actionNode = (WorkflowActionNode) fork2Branch2.get(0);
    Assert.assertEquals("a4", actionNode.getNodeId());
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.CUSTOM_ACTION,
                                                                             "DummyAction")));
    Assert.assertNotNull(actionNode.getActionSpecification());

    actionNode = (WorkflowActionNode) fork1Branch2.get(0);
    Assert.assertEquals("a5", actionNode.getNodeId());
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.CUSTOM_ACTION,
                                                                             "DummyAction")));
    Assert.assertNotNull(actionNode.getActionSpecification());

    node = nodes.get(2);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    actionNode = (WorkflowActionNode) node;
    Assert.assertEquals("mr6", actionNode.getNodeId());
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "DummyMR")));
    Assert.assertNull(actionNode.getActionSpecification());

    node = nodes.get(3);
    WorkflowForkNode fork5 = (WorkflowForkNode) node;
    List<WorkflowNode> fork5Branch1 = fork5.getBranches().get(0);
    Assert.assertTrue(fork5Branch1.size() == 1);
    List<WorkflowNode> fork5Branch2 = fork5.getBranches().get(1);
    Assert.assertTrue(fork5Branch2.size() == 1);

    actionNode = (WorkflowActionNode) fork5Branch1.get(0);
    Assert.assertEquals("a6", actionNode.getNodeId());
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.CUSTOM_ACTION,
                                                                             "DummyAction")));
    Assert.assertNotNull(actionNode.getActionSpecification());

    actionNode = (WorkflowActionNode) fork5Branch2.get(0);
    Assert.assertEquals("mr7", actionNode.getNodeId());
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "DummyMR")));
    Assert.assertNull(actionNode.getActionSpecification());
  }
}
