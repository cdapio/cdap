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
    verifyWorkflowWithForkInConditionSpecification(appSpec);
    ApplicationSpecificationAdapter adapter = ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator());
    ApplicationSpecification newSpec = adapter.fromJson(adapter.toJson(appSpec));
    verifyGoodWorkflowSpecifications(newSpec);
    verifyAnotherGoodWorkflowSpecification(appSpec);
    verifyWorkflowWithForkInConditionSpecification(appSpec);
  }

  private void verifyAnotherGoodWorkflowSpecification(ApplicationSpecification appSpec) {
    WorkflowSpecification spec = appSpec.getWorkflows().get("AnotherGoodWorkflow");
    List<WorkflowNode> nodes = spec.getNodes();
    Assert.assertTrue(nodes.size() == 4);

    WorkflowNode node = nodes.get(0);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    WorkflowActionNode actionNode = (WorkflowActionNode) node;
    Assert.assertEquals(0, node.getParentNodeIds().size());
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
    actionNode = (WorkflowActionNode) node;
    Assert.assertEquals(1, node.getParentNodeIds().size());
    Assert.assertEquals("MR1", node.getParentNodeIds().iterator().next());
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "MR2")));
    node = forkBranch1.get(1);
    Assert.assertTrue(node.getType() == WorkflowNodeType.CONDITION);
    WorkflowConditionNode condition = (WorkflowConditionNode) node;
    Assert.assertEquals(1, node.getParentNodeIds().size());
    Assert.assertEquals("MR2", node.getParentNodeIds().iterator().next());
    Assert.assertTrue(node.getNodeId().equals("MyVerificationPredicate"));
    Assert.assertTrue(condition.getPredicateClassName().contains("MyVerificationPredicate"));

    List<WorkflowNode> ifNodes = condition.getIfBranch();
    Assert.assertTrue(ifNodes.size() == 2);
    List<WorkflowNode> elseNodes = condition.getElseBranch();
    Assert.assertTrue(elseNodes.size() == 2);

    node = ifNodes.get(0);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    actionNode = (WorkflowActionNode) node;
    Assert.assertEquals(1, node.getParentNodeIds().size());
    Assert.assertEquals("MyVerificationPredicate", node.getParentNodeIds().iterator().next());
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "MR3")));

    node = ifNodes.get(1);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    actionNode = (WorkflowActionNode) node;
    Assert.assertEquals(1, node.getParentNodeIds().size());
    Assert.assertEquals("MR3", node.getParentNodeIds().iterator().next());
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "MR4")));

    node = elseNodes.get(0);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    actionNode = (WorkflowActionNode) node;
    Assert.assertEquals(1, node.getParentNodeIds().size());
    Assert.assertEquals("MyVerificationPredicate", node.getParentNodeIds().iterator().next());
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "MR5")));

    node = elseNodes.get(1);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    actionNode = (WorkflowActionNode) node;
    Assert.assertEquals(1, node.getParentNodeIds().size());
    Assert.assertEquals("MR5", node.getParentNodeIds().iterator().next());
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "MR6")));
    node = forkBranch1.get(2);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    actionNode = (WorkflowActionNode) node;
    Assert.assertEquals(2, node.getParentNodeIds().size());
    Assert.assertTrue(node.getParentNodeIds().contains("MR4"));
    Assert.assertTrue(node.getParentNodeIds().contains("MR6"));
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "MR7")));

    List<WorkflowNode> forkBranch2 = fork.getBranches().get(1);
    Assert.assertTrue(forkBranch2.size() == 1);

    node = forkBranch2.get(0);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    actionNode = (WorkflowActionNode) node;
    Assert.assertEquals(1, node.getParentNodeIds().size());
    Assert.assertEquals("MR1", node.getParentNodeIds().iterator().next());
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "MR8")));

    node = nodes.get(2);
    Assert.assertTrue(node.getType() == WorkflowNodeType.CONDITION);
    Assert.assertEquals(2, node.getParentNodeIds().size());
    Assert.assertTrue(node.getParentNodeIds().contains("MR7"));
    Assert.assertTrue(node.getParentNodeIds().contains("MR8"));
    Assert.assertTrue(node.getNodeId().equals("AnotherVerificationPredicate"));
    condition = (WorkflowConditionNode) node;
    Assert.assertTrue(condition.getPredicateClassName().contains("AnotherVerificationPredicate"));

    ifNodes = ((WorkflowConditionNode) node).getIfBranch();
    elseNodes = ((WorkflowConditionNode) node).getElseBranch();

    Assert.assertTrue(ifNodes.size() == 2);
    node = ifNodes.get(0);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    actionNode = (WorkflowActionNode) node;
    Assert.assertEquals(1, node.getParentNodeIds().size());
    Assert.assertEquals("AnotherVerificationPredicate", node.getParentNodeIds().iterator().next());
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.SPARK,
                                                                             "SP1")));

    node = ifNodes.get(1);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    actionNode = (WorkflowActionNode) node;
    Assert.assertEquals(1, node.getParentNodeIds().size());
    Assert.assertEquals("SP1", node.getParentNodeIds().iterator().next());
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.SPARK,
                                                                             "SP2")));

    Assert.assertTrue(elseNodes.size() == 3);

    node = elseNodes.get(0);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    actionNode = (WorkflowActionNode) node;
    Assert.assertEquals(1, node.getParentNodeIds().size());
    Assert.assertEquals("AnotherVerificationPredicate", node.getParentNodeIds().iterator().next());
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.SPARK,
                                                                             "SP3")));

    node = elseNodes.get(1);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    actionNode = (WorkflowActionNode) node;
    Assert.assertEquals(1, node.getParentNodeIds().size());
    Assert.assertEquals("SP3", node.getParentNodeIds().iterator().next());
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
    actionNode = (WorkflowActionNode) node;
    Assert.assertEquals(1, node.getParentNodeIds().size());
    Assert.assertEquals("SP4", node.getParentNodeIds().iterator().next());
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.SPARK,
                                                                             "SP5")));

    node = anotherForkBranch2.get(0);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    actionNode = (WorkflowActionNode) node;
    Assert.assertEquals(1, node.getParentNodeIds().size());
    Assert.assertEquals("SP4", node.getParentNodeIds().iterator().next());
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.SPARK,
                                                                             "SP6")));

    node = nodes.get(3);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    actionNode = (WorkflowActionNode) node;
    Assert.assertEquals(3, node.getParentNodeIds().size());
    Assert.assertTrue(node.getParentNodeIds().contains("SP2"));
    Assert.assertTrue(node.getParentNodeIds().contains("SP5"));
    Assert.assertTrue(node.getParentNodeIds().contains("SP6"));
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.SPARK,
                                                                             "SP7")));
  }

  private void verifyGoodWorkflowSpecifications(ApplicationSpecification appSpec) {
    WorkflowSpecification spec = appSpec.getWorkflows().get("GoodWorkflow");
    Assert.assertTrue(spec.getNodes().size() == 4);
    List<WorkflowNode> nodes = spec.getNodes();

    WorkflowNode node = nodes.get(0);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    WorkflowActionNode actionNode = (WorkflowActionNode) node;
    Assert.assertEquals(0, node.getParentNodeIds().size());
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.CUSTOM_ACTION,
                                                                             "DA1")));
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
    Assert.assertEquals(1, fork1Branch1.get(0).getParentNodeIds().size());
    Assert.assertEquals("DA1", fork1Branch1.get(0).getParentNodeIds().iterator().next());
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "MR1")));
    Assert.assertNull(actionNode.getActionSpecification());

    WorkflowForkNode fork2 = (WorkflowForkNode) fork1Branch1.get(1);
    List<WorkflowNode> fork2Branch1 = fork2.getBranches().get(0);
    Assert.assertTrue(fork2Branch1.size() == 2);
    List<WorkflowNode> fork2Branch2 = fork2.getBranches().get(1);
    Assert.assertTrue(fork2Branch2.size() == 1);

    actionNode = (WorkflowActionNode) fork2Branch1.get(0);
    Assert.assertEquals(1, fork2Branch1.get(0).getParentNodeIds().size());
    Assert.assertEquals("MR1", fork2Branch1.get(0).getParentNodeIds().iterator().next());
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.CUSTOM_ACTION,
                                                                             "DA2")));
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
    Assert.assertEquals(1, fork4Branch1.get(0).getParentNodeIds().size());
    Assert.assertEquals("DA2", fork4Branch1.get(0).getParentNodeIds().iterator().next());
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "MR2")));
    Assert.assertNull(actionNode.getActionSpecification());

    actionNode = (WorkflowActionNode) fork4Branch1.get(1);
    Assert.assertEquals(1, fork4Branch1.get(1).getParentNodeIds().size());
    Assert.assertEquals("MR2", fork4Branch1.get(1).getParentNodeIds().iterator().next());
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.CUSTOM_ACTION,
                                                                             "DA3")));
    Assert.assertNotNull(actionNode.getActionSpecification());

    actionNode = (WorkflowActionNode) fork4Branch2.get(0);
    Assert.assertEquals(1, fork4Branch2.get(0).getParentNodeIds().size());
    Assert.assertEquals("DA2", fork4Branch2.get(0).getParentNodeIds().iterator().next());
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "MR3")));
    Assert.assertNull(actionNode.getActionSpecification());

    actionNode = (WorkflowActionNode) fork3Branch1.get(1);
    Assert.assertEquals(2, fork3Branch1.get(1).getParentNodeIds().size());
    Assert.assertTrue(fork3Branch1.get(1).getParentNodeIds().contains("DA3"));
    Assert.assertTrue(fork3Branch1.get(1).getParentNodeIds().contains("MR3"));
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "MR4")));

    actionNode = (WorkflowActionNode) fork3Branch2.get(0);
    Assert.assertEquals(1, fork3Branch2.get(0).getParentNodeIds().size());
    Assert.assertEquals("DA2", fork3Branch2.get(0).getParentNodeIds().iterator().next());
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "MR5")));

    actionNode = (WorkflowActionNode) fork2Branch2.get(0);
    Assert.assertEquals(1, fork2Branch2.get(0).getParentNodeIds().size());
    Assert.assertEquals("MR1", fork2Branch2.get(0).getParentNodeIds().iterator().next());
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.CUSTOM_ACTION,
                                                                             "DA4")));
    Assert.assertNotNull(actionNode.getActionSpecification());

    actionNode = (WorkflowActionNode) fork1Branch2.get(0);
    Assert.assertEquals(1, fork1Branch2.get(0).getParentNodeIds().size());
    Assert.assertEquals("DA1", fork1Branch2.get(0).getParentNodeIds().iterator().next());
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.CUSTOM_ACTION,
                                                                             "DA5")));
    Assert.assertNotNull(actionNode.getActionSpecification());

    node = nodes.get(2);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    actionNode = (WorkflowActionNode) node;
    Assert.assertEquals(4, node.getParentNodeIds().size());
    Assert.assertTrue(node.getParentNodeIds().contains("MR4"));
    Assert.assertTrue(node.getParentNodeIds().contains("MR5"));
    Assert.assertTrue(node.getParentNodeIds().contains("DA4"));
    Assert.assertTrue(node.getParentNodeIds().contains("DA5"));
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "MR6")));
    Assert.assertNull(actionNode.getActionSpecification());

    node = nodes.get(3);
    WorkflowForkNode fork5 = (WorkflowForkNode) node;
    List<WorkflowNode> fork5Branch1 = fork5.getBranches().get(0);
    Assert.assertTrue(fork5Branch1.size() == 1);
    List<WorkflowNode> fork5Branch2 = fork5.getBranches().get(1);
    Assert.assertTrue(fork5Branch2.size() == 1);

    actionNode = (WorkflowActionNode) fork5Branch1.get(0);
    Assert.assertEquals(1, fork5Branch1.get(0).getParentNodeIds().size());
    Assert.assertEquals("MR6", fork5Branch1.get(0).getParentNodeIds().iterator().next());
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.CUSTOM_ACTION,
                                                                             "DA6")));
    Assert.assertNotNull(actionNode.getActionSpecification());

    actionNode = (WorkflowActionNode) fork5Branch2.get(0);
    Assert.assertEquals(1, fork5Branch2.get(0).getParentNodeIds().size());
    Assert.assertEquals("MR6", fork5Branch2.get(0).getParentNodeIds().iterator().next());
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "MR7")));
    Assert.assertNull(actionNode.getActionSpecification());
  }

  private void verifyWorkflowWithForkInConditionSpecification(ApplicationSpecification appSpec) {
    WorkflowSpecification spec = appSpec.getWorkflows().get("WorkflowWithForkInCondition");
    List<WorkflowNode> nodes = spec.getNodes();
    Assert.assertTrue(nodes.size() == 2);

    WorkflowNode node = spec.getNodeIdMap().get("MyVerificationPredicate");
    Assert.assertTrue(node.getType() == WorkflowNodeType.CONDITION);
    Assert.assertEquals(0, node.getParentNodeIds().size());
    WorkflowConditionNode conditionNode  = (WorkflowConditionNode) node;
    Assert.assertTrue(conditionNode.getNodeId().equals("MyVerificationPredicate"));
    Assert.assertTrue(conditionNode.getPredicateClassName().contains("MyVerificationPredicate"));

    node = spec.getNodeIdMap().get("MR1");
    Assert.assertEquals(1, node.getParentNodeIds().size());
    Assert.assertEquals("MyVerificationPredicate", node.getParentNodeIds().iterator().next());

    node = spec.getNodeIdMap().get("MR2");
    Assert.assertEquals(1, node.getParentNodeIds().size());
    Assert.assertEquals("MR1", node.getParentNodeIds().iterator().next());

    node = spec.getNodeIdMap().get("MR3");
    Assert.assertEquals(1, node.getParentNodeIds().size());
    Assert.assertEquals("MyVerificationPredicate", node.getParentNodeIds().iterator().next());

    node = spec.getNodeIdMap().get("SP1");
    Assert.assertEquals(1, node.getParentNodeIds().size());
    Assert.assertEquals("MyVerificationPredicate", node.getParentNodeIds().iterator().next());

    node = spec.getNodeIdMap().get("SP2");
    Assert.assertEquals(1, node.getParentNodeIds().size());
    Assert.assertEquals("MyVerificationPredicate", node.getParentNodeIds().iterator().next());

    node = spec.getNodeIdMap().get("MR4");
    Assert.assertEquals(4, node.getParentNodeIds().size());
    Assert.assertTrue(node.getParentNodeIds().contains("MR2"));
    Assert.assertTrue(node.getParentNodeIds().contains("MR3"));
    Assert.assertTrue(node.getParentNodeIds().contains("SP2"));
    Assert.assertTrue(node.getParentNodeIds().contains("SP2"));
  }
}
