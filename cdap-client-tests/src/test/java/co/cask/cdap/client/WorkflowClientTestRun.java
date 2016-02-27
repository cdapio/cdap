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

package co.cask.cdap.client;

import co.cask.cdap.AppWithWorkflow;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.client.common.ClientTestBase;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.WorkflowTokenDetail;
import co.cask.cdap.proto.WorkflowTokenNodeDetail;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link WorkflowClient}
 */
public class WorkflowClientTestRun extends ClientTestBase {

  private static final Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, AppWithWorkflow.NAME);

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private ApplicationClient appClient;
  private ProgramClient programClient;
  private WorkflowClient workflowClient;

  @Before
  public void setUp() throws Throwable {
    super.setUp();
    appClient = new ApplicationClient(clientConfig);
    programClient = new ProgramClient(clientConfig);
    workflowClient = new WorkflowClient(clientConfig);
    appClient.deploy(Id.Namespace.DEFAULT, createAppJarFile(AppWithWorkflow.class));
  }

  @After
  public void tearDown() throws Exception {
    appClient.delete(appId);
  }

  @Test
  public void testWorkflowClient() throws Exception {
    String outputPath = new File(tmpFolder.newFolder(), "output").getAbsolutePath();
    Map<String, String> runtimeArgs = ImmutableMap.of("inputPath", createInput("input"),
                                                      "outputPath", outputPath);
    final Id.Workflow workflowId = Id.Workflow.from(appId, AppWithWorkflow.SampleWorkflow.NAME);
    programClient.start(workflowId, false, runtimeArgs);
    programClient.waitForStatus(workflowId, "STOPPED", 60, TimeUnit.SECONDS);

    Tasks.waitFor(1, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return programClient.getProgramRuns(workflowId,
                                            ProgramRunStatus.COMPLETED.name(), 0, Long.MAX_VALUE, 10).size();
      }
    }, 10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

    List<RunRecord> workflowRuns = programClient.getProgramRuns(workflowId, ProgramRunStatus.COMPLETED.name(), 0,
                                                                Long.MAX_VALUE, 10);
    Assert.assertEquals(1, workflowRuns.size());
    Id.Run workflowRunId = new Id.Run(workflowId, workflowRuns.get(0).getPid());

    // Invalid test scenarios
    try {
      workflowClient.getWorkflowToken(new Id.Run(Id.Workflow.from(appId, "random"), workflowRunId.getId()));
      Assert.fail("Should not find a workflow token for a non-existing workflow");
    } catch (NotFoundException expected) {
      // expected
    }
    try {
      workflowClient.getWorkflowToken(new Id.Run(workflowId, RunIds.generate().getId()));
      Assert.fail("Should not find a workflow token for a random run id");
    } catch (NotFoundException expected) {
      // expected
    }

    // Valid test scenarios
    WorkflowTokenDetail workflowToken = workflowClient.getWorkflowToken(workflowRunId);
    Assert.assertEquals(3, workflowToken.getTokenData().size());
    workflowToken = workflowClient.getWorkflowToken(workflowRunId, WorkflowToken.Scope.SYSTEM);
    Assert.assertTrue(workflowToken.getTokenData().size() > 0);
    workflowToken = workflowClient.getWorkflowToken(workflowRunId, "start_time");
    Map<String, List<WorkflowTokenDetail.NodeValueDetail>> tokenData = workflowToken.getTokenData();
    Assert.assertEquals(AppWithWorkflow.WordCountMapReduce.NAME, tokenData.get("start_time").get(0).getNode());
    Assert.assertTrue(Long.parseLong(tokenData.get("start_time").get(0).getValue()) < System.currentTimeMillis());
    workflowToken = workflowClient.getWorkflowToken(workflowRunId, WorkflowToken.Scope.USER, "action_type");
    tokenData = workflowToken.getTokenData();
    Assert.assertEquals(AppWithWorkflow.WordCountMapReduce.NAME, tokenData.get("action_type").get(0).getNode());
    Assert.assertEquals("MapReduce", tokenData.get("action_type").get(0).getValue());
    String nodeName = AppWithWorkflow.SampleWorkflow.firstActionName;
    WorkflowTokenNodeDetail workflowTokenAtNode =
      workflowClient.getWorkflowTokenAtNode(workflowRunId, nodeName);
    Assert.assertEquals(AppWithWorkflow.DummyAction.TOKEN_VALUE,
                        workflowTokenAtNode.getTokenDataAtNode().get(AppWithWorkflow.DummyAction.TOKEN_KEY));
    workflowTokenAtNode = workflowClient.getWorkflowTokenAtNode(workflowRunId, nodeName, WorkflowToken.Scope.SYSTEM);
    Assert.assertEquals(0, workflowTokenAtNode.getTokenDataAtNode().size());
    workflowTokenAtNode = workflowClient.getWorkflowTokenAtNode(workflowRunId, nodeName,
                                                                AppWithWorkflow.DummyAction.TOKEN_KEY);
    Assert.assertEquals(AppWithWorkflow.DummyAction.TOKEN_VALUE,
                        workflowTokenAtNode.getTokenDataAtNode().get(AppWithWorkflow.DummyAction.TOKEN_KEY));
    String reduceOutputRecordsCounter = "org.apache.hadoop.mapreduce.TaskCounter.REDUCE_OUTPUT_RECORDS";
    workflowTokenAtNode = workflowClient.getWorkflowTokenAtNode(workflowRunId, AppWithWorkflow.WordCountMapReduce.NAME,
                                                                WorkflowToken.Scope.SYSTEM, reduceOutputRecordsCounter);
    Assert.assertEquals(6, Integer.parseInt(workflowTokenAtNode.getTokenDataAtNode().get(reduceOutputRecordsCounter)));
  }

  private String createInput(String folderName) throws IOException {
    File inputDir = tmpFolder.newFolder(folderName);

    File inputFile = new File(inputDir.getPath() + "/words.txt");
    try (BufferedWriter writer = Files.newBufferedWriter(inputFile.toPath(), Charsets.UTF_8)) {
      writer.write("this text has");
      writer.newLine();
      writer.write("two words text inside");
    }
    return inputDir.getAbsolutePath();
  }
}
