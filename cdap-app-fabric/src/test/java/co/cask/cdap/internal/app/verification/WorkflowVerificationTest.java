/*
 * Copyright © 2015-2016 Cask Data, Inc.
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
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.workflow.ScheduleProgramInfo;
import co.cask.cdap.api.workflow.WorkflowActionNode;
import co.cask.cdap.api.workflow.WorkflowConditionNode;
import co.cask.cdap.api.workflow.WorkflowForkNode;
import co.cask.cdap.api.workflow.WorkflowNode;
import co.cask.cdap.api.workflow.WorkflowNodeType;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.deploy.Specifications;
import co.cask.cdap.internal.dataset.DatasetCreationSpec;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 *
 */
public class WorkflowVerificationTest {
  @Test
  public void testGoodWorkflow() throws Exception {
    ApplicationSpecification appSpec = Specifications.from(new GoodWorkflowApp());
    verifyGoodWorkflowSpecifications(appSpec);
    verifyAnotherGoodWorkflowSpecification(appSpec);
    verifyWorkflowWithLocalDatasetSpecification(appSpec);
    ApplicationSpecificationAdapter adapter = ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator());
    ApplicationSpecification newSpec = adapter.fromJson(adapter.toJson(appSpec));
    verifyGoodWorkflowSpecifications(newSpec);
    verifyAnotherGoodWorkflowSpecification(newSpec);
    verifyWorkflowWithLocalDatasetSpecification(newSpec);
  }

  private void verifyWorkflowWithLocalDatasetSpecification(ApplicationSpecification appSpec) {
    WorkflowSpecification spec = appSpec.getWorkflows().get("WorkflowWithLocalDatasets");
    List<WorkflowNode> nodes = spec.getNodes();
    Assert.assertTrue(nodes.size() == 2);

    WorkflowNode node = nodes.get(0);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    WorkflowActionNode actionNode = (WorkflowActionNode) node;
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "MR1")));

    node = nodes.get(1);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    actionNode = (WorkflowActionNode) node;
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.SPARK,
                                                                             "SP1")));

    Map<String, DatasetCreationSpec> localDatasetSpecs = spec.getLocalDatasetSpecs();
    Assert.assertEquals(5, localDatasetSpecs.size());

    DatasetCreationSpec datasetCreationSpec = localDatasetSpecs.get("mytable");
    Assert.assertEquals(Table.class.getName(), datasetCreationSpec.getTypeName());
    Assert.assertEquals(0, datasetCreationSpec.getProperties().getProperties().size());

    datasetCreationSpec = localDatasetSpecs.get("myfile");
    Assert.assertEquals(FileSet.class.getName(), datasetCreationSpec.getTypeName());
    Assert.assertEquals(0, datasetCreationSpec.getProperties().getProperties().size());

    datasetCreationSpec = localDatasetSpecs.get("myfile_with_properties");
    Assert.assertEquals(FileSet.class.getName(), datasetCreationSpec.getTypeName());
    Assert.assertEquals("prop_value", datasetCreationSpec.getProperties().getProperties().get("prop_key"));

    datasetCreationSpec = localDatasetSpecs.get("mytablefromtype");
    Assert.assertEquals(Table.class.getName(), datasetCreationSpec.getTypeName());
    Assert.assertEquals(0, datasetCreationSpec.getProperties().getProperties().size());

    datasetCreationSpec = localDatasetSpecs.get("myfilefromtype");
    Assert.assertEquals(FileSet.class.getName(), datasetCreationSpec.getTypeName());
    Assert.assertEquals("another_prop_value",
                        datasetCreationSpec.getProperties().getProperties().get("another_prop_key"));

    // Check if the application specification has correct modules
    Map<String, String> datasetModules = appSpec.getDatasetModules();
    Assert.assertEquals(2, datasetModules.size());
    Assert.assertTrue(datasetModules.containsKey(FileSet.class.getName()));
    Assert.assertTrue(datasetModules.containsKey(Table.class.getName()));
  }

  private void verifyAnotherGoodWorkflowSpecification(ApplicationSpecification appSpec) {
    WorkflowSpecification spec = appSpec.getWorkflows().get("AnotherGoodWorkflow");
    List<WorkflowNode> nodes = spec.getNodes();
    Assert.assertTrue(nodes.size() == 4);

    WorkflowNode node = nodes.get(0);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
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
    actionNode = (WorkflowActionNode) node;
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "MR2")));
    node = forkBranch1.get(1);
    Assert.assertTrue(node.getType() == WorkflowNodeType.CONDITION);

    WorkflowConditionNode condition = (WorkflowConditionNode) node;
    Assert.assertTrue(condition.getPredicateClassName().contains("MyVerificationPredicate"));
    List<WorkflowNode> ifNodes = condition.getIfBranch();
    Assert.assertTrue(ifNodes.size() == 2);
    List<WorkflowNode> elseNodes = condition.getElseBranch();
    Assert.assertTrue(elseNodes.size() == 2);

    node = ifNodes.get(0);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    actionNode = (WorkflowActionNode) node;
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "MR3")));

    node = ifNodes.get(1);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    actionNode = (WorkflowActionNode) node;
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "MR4")));

    node = elseNodes.get(0);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    actionNode = (WorkflowActionNode) node;
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "MR5")));

    node = elseNodes.get(1);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    actionNode = (WorkflowActionNode) node;
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "MR6")));
    node = forkBranch1.get(2);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    actionNode = (WorkflowActionNode) node;
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "MR7")));

    List<WorkflowNode> forkBranch2 = fork.getBranches().get(1);
    Assert.assertTrue(forkBranch2.size() == 1);

    node = forkBranch2.get(0);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    actionNode = (WorkflowActionNode) node;
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "MR8")));

    node = nodes.get(2);
    Assert.assertTrue(node.getType() == WorkflowNodeType.CONDITION);
    ifNodes = ((WorkflowConditionNode) node).getIfBranch();
    elseNodes = ((WorkflowConditionNode) node).getElseBranch();

    Assert.assertTrue(ifNodes.size() == 2);
    node = ifNodes.get(0);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    actionNode = (WorkflowActionNode) node;
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.SPARK,
                                                                             "SP1")));

    node = ifNodes.get(1);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    actionNode = (WorkflowActionNode) node;
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.SPARK,
                                                                             "SP2")));

    Assert.assertTrue(elseNodes.size() == 3);

    node = elseNodes.get(0);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    actionNode = (WorkflowActionNode) node;
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.SPARK,
                                                                             "SP3")));

    node = elseNodes.get(1);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
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
    actionNode = (WorkflowActionNode) node;
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.SPARK,
                                                                             "SP5")));

    node = anotherForkBranch2.get(0);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    actionNode = (WorkflowActionNode) node;
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.SPARK,
                                                                             "SP6")));

    node = nodes.get(3);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
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
    WorkflowActionNode actionNode = (WorkflowActionNode) node;
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.CUSTOM_ACTION,
                                                                             "DummyAction")));
    Assert.assertNotNull(actionNode.getCustomActionSpecification());

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
    Assert.assertNull(actionNode.getActionSpecification());

    WorkflowForkNode fork2 = (WorkflowForkNode) fork1Branch1.get(1);
    List<WorkflowNode> fork2Branch1 = fork2.getBranches().get(0);
    Assert.assertTrue(fork2Branch1.size() == 2);
    List<WorkflowNode> fork2Branch2 = fork2.getBranches().get(1);
    Assert.assertTrue(fork2Branch2.size() == 1);

    actionNode = (WorkflowActionNode) fork2Branch1.get(0);
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.CUSTOM_ACTION,
                                                                             "DummyAction")));
    Assert.assertNotNull(actionNode.getCustomActionSpecification());

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
    Assert.assertNull(actionNode.getActionSpecification());

    actionNode = (WorkflowActionNode) fork4Branch1.get(1);
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.CUSTOM_ACTION,
                                                                             "DummyAction")));
    Assert.assertNotNull(actionNode.getCustomActionSpecification());

    actionNode = (WorkflowActionNode) fork4Branch2.get(0);
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "DummyMR")));
    Assert.assertNull(actionNode.getCustomActionSpecification());

    actionNode = (WorkflowActionNode) fork4Branch1.get(0);
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "DummyMR")));
    Assert.assertNull(actionNode.getCustomActionSpecification());

    actionNode = (WorkflowActionNode) fork4Branch2.get(0);
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "DummyMR")));
    Assert.assertNull(actionNode.getCustomActionSpecification());

    actionNode = (WorkflowActionNode) fork2Branch2.get(0);
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.CUSTOM_ACTION,
                                                                             "DummyAction")));
    Assert.assertNotNull(actionNode.getCustomActionSpecification());

    actionNode = (WorkflowActionNode) fork1Branch2.get(0);
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.CUSTOM_ACTION,
                                                                             "DummyAction")));
    Assert.assertNotNull(actionNode.getCustomActionSpecification());

    node = nodes.get(2);
    Assert.assertTrue(node.getType() == WorkflowNodeType.ACTION);
    actionNode = (WorkflowActionNode) node;
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "DummyMR")));
    Assert.assertNull(actionNode.getCustomActionSpecification());

    node = nodes.get(3);
    WorkflowForkNode fork5 = (WorkflowForkNode) node;
    List<WorkflowNode> fork5Branch1 = fork5.getBranches().get(0);
    Assert.assertTrue(fork5Branch1.size() == 1);
    List<WorkflowNode> fork5Branch2 = fork5.getBranches().get(1);
    Assert.assertTrue(fork5Branch2.size() == 1);

    actionNode = (WorkflowActionNode) fork5Branch1.get(0);
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.CUSTOM_ACTION,
                                                                             "DummyAction")));
    Assert.assertNotNull(actionNode.getCustomActionSpecification());

    actionNode = (WorkflowActionNode) fork5Branch2.get(0);
    Assert.assertTrue(actionNode.getProgram().equals(new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE,
                                                                             "DummyMR")));
    Assert.assertNull(actionNode.getCustomActionSpecification());
  }
}
