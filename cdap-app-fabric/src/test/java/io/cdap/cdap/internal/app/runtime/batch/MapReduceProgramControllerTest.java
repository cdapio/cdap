/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.batch;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.lineage.field.EndPoint;
import io.cdap.cdap.api.lineage.field.ReadOperation;
import io.cdap.cdap.api.mapreduce.MapReduceSpecification;
import io.cdap.cdap.api.metadata.MetadataReader;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreManager;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.metadata.writer.FieldLineageWriter;
import io.cdap.cdap.data2.metadata.writer.MetadataPublisher;
import io.cdap.cdap.internal.app.DefaultApplicationSpecification;
import io.cdap.cdap.internal.app.runtime.AppStateStoreProvider;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.workflow.BasicWorkflowToken;
import io.cdap.cdap.internal.app.runtime.workflow.WorkflowProgramInfo;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import java.io.IOException;
import java.util.Collections;
import javax.annotation.Nullable;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link MapReduceProgramController}.
 */
public class MapReduceProgramControllerTest {

  @Test
  public void testGetWorkflowTokenThrowsWhenNotInWorkflow() {
    BasicMapReduceContext context = createContext(null);
    MapReduceProgramController controller =
        new MapReduceProgramController(createNoOpService(), context);
    try {
      controller.getWorkflowToken();
      Assert.fail("Expected IllegalStateException");
    } catch (IllegalStateException expected) {
      Assert.assertTrue(expected.getMessage().contains("WorkflowToken cannot be null"));
    } finally {
      context.close();
    }
  }

  @Test
  public void testGetWorkflowTokenPopulatesCountersAndDelegatesLineage() throws Exception {
    WorkflowProgramInfo wfInfo = createWorkflowProgramInfo();
    BasicMapReduceContext context = createContext(wfInfo);

    try {
      Job job = Mockito.mock(Job.class);
      Counters counters = new Counters();
      counters.findCounter("group", "records").increment(7L);
      Mockito.when(job.getCounters()).thenReturn(counters);
      context.setJob(job);

      ReadOperation readOp = new ReadOperation(
          "readInput", "desc", EndPoint.of("default", "srcFile"), "col1");
      context.record(Collections.singletonList(readOp));

      MapReduceProgramController controller =
          new MapReduceProgramController(createNoOpService(), context);
      WorkflowToken actualToken = controller.getWorkflowToken();

      Assert.assertSame(wfInfo.getWorkflowToken(), actualToken);
      Assert.assertEquals(
          7L, actualToken.get("group.records", WorkflowToken.Scope.SYSTEM).getAsLong());
      Assert.assertEquals(Collections.singleton(readOp), controller.getFieldLineageOperations());
    } finally {
      context.close();
    }
  }

  @Test
  public void testGetWorkflowTokenWrapsIoExceptionAndPreservesRuntimeException() throws Exception {
    WorkflowProgramInfo wfInfo = createWorkflowProgramInfo();
    BasicMapReduceContext context = createContext(wfInfo);

    try {
      Job job = Mockito.mock(Job.class);
      context.setJob(job);
      IOException ioFailure = new IOException("counter fetch failed");
      Mockito.when(job.getCounters()).thenThrow(ioFailure);

      MapReduceProgramController controller =
          new MapReduceProgramController(createNoOpService(), context);
      try {
        controller.getWorkflowToken();
        Assert.fail("Expected RuntimeException wrapping IOException");
      } catch (RuntimeException thrown) {
        Assert.assertSame(ioFailure, thrown.getCause());
      }

      IllegalStateException runtimeFailure = new IllegalStateException("job not initialized");
      Mockito.doThrow(runtimeFailure).when(job).getCounters();
      try {
        controller.getWorkflowToken();
        Assert.fail("Expected IllegalStateException");
      } catch (RuntimeException thrown) {
        Assert.assertSame(runtimeFailure, thrown);
      }
    } finally {
      context.close();
    }
  }

  static BasicMapReduceContext createContext(@Nullable WorkflowProgramInfo wfInfo) {
    final CConfiguration cConf = CConfiguration.create();
    ProgramId programId = NamespaceId.DEFAULT.app("mrApp").mr("wordCountMr");
    MapReduceSpecification mrSpec = new MapReduceSpecification(
        "io.cdap.cdap.DummyMr", "wordCountMr", "desc", null, null,
        Collections.emptySet(), Collections.emptyMap(), null, null, null, Collections.emptyMap());
    ArtifactId artifactId = new ArtifactId("art", new ArtifactVersion("1.0.0"), ArtifactScope.USER);
    ApplicationSpecification appSpec = new DefaultApplicationSpecification(
        "mrApp", "1.0.0", "desc", null, artifactId,
        Collections.emptyMap(), Collections.emptyMap(),
        ImmutableMap.of("wordCountMr", mrSpec), Collections.emptyMap(),
        Collections.emptyMap(), Collections.emptyMap(),
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());

    Program program = Mockito.mock(Program.class);
    Mockito.when(program.getId()).thenReturn(programId);
    Mockito.when(program.getName()).thenReturn(programId.getProgram());
    Mockito.when(program.getNamespaceId()).thenReturn(programId.getNamespace());
    Mockito.when(program.getApplicationId()).thenReturn(programId.getApplication());
    Mockito.when(program.getType()).thenReturn(ProgramType.MAPREDUCE);
    Mockito.when(program.getApplicationSpecification()).thenReturn(appSpec);
    Mockito.when(program.getClassLoader()).thenReturn(
        MapReduceProgramControllerTest.class.getClassLoader());

    ProgramOptions options = new SimpleProgramOptions(
        programId,
        new BasicArguments(ImmutableMap.of(
            ProgramOptionConstants.RUN_ID, RunIds.generate().getId(),
            ProgramOptionConstants.ARTIFACT_ID, "default:art:1.0.0")),
        new BasicArguments());

    return new BasicMapReduceContext(
        program,
        options,
        cConf,
        mrSpec,
        wfInfo,
        Mockito.mock(DiscoveryServiceClient.class),
        new NoOpMetricsCollectionService(),
        Mockito.mock(TransactionSystemClient.class),
        Mockito.mock(DatasetFramework.class),
        null,
        null,
        Mockito.mock(SecureStore.class),
        Mockito.mock(SecureStoreManager.class),
        Mockito.mock(MessagingService.class),
        Mockito.mock(MetadataReader.class),
        Mockito.mock(MetadataPublisher.class),
        Mockito.mock(NamespaceQueryAdmin.class),
        Mockito.mock(FieldLineageWriter.class),
        Mockito.mock(RemoteClientFactory.class),
        Mockito.mock(AppStateStoreProvider.class));
  }

  private WorkflowProgramInfo createWorkflowProgramInfo() {
    BasicWorkflowToken token = new BasicWorkflowToken(1);
    token.setCurrentNode("mrNode");
    return WorkflowProgramInfo.create(new BasicArguments(ImmutableMap.of(
        ProgramOptionConstants.WORKFLOW_NAME, "wfName",
        ProgramOptionConstants.WORKFLOW_NODE_ID, "mrNode",
        ProgramOptionConstants.WORKFLOW_RUN_ID, RunIds.generate().getId(),
        ProgramOptionConstants.PROGRAM_NAME_IN_WORKFLOW, "wordCountMr",
        ProgramOptionConstants.WORKFLOW_TOKEN, new Gson().toJson(token))));
  }

  private Service createNoOpService() {
    return new AbstractIdleService() {
      @Override
      protected void startUp() {
        // no-op
      }

      @Override
      protected void shutDown() {
        // no-op
      }
    };
  }
}
