/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.support.tasks;

import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.logging.gateway.handlers.RemoteLogsFetcher;
import io.cdap.cdap.logging.gateway.handlers.RemoteProgramRunRecordFetcher;
import io.cdap.cdap.logging.gateway.handlers.RemoteProgramRunRecordsFetcher;
import io.cdap.cdap.metadata.RemoteApplicationDetailFetcher;
import io.cdap.cdap.metrics.process.RemoteMetricsSystemClient;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.support.job.SupportBundleJob;
import io.cdap.cdap.support.task.SupportBundlePipelineInfoTask;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.function.Consumer;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Fast unit tests for {@link SupportBundlePipelineInfoTask} scan lambda exception propagation.
 */
public class SupportBundlePipelineInfoTaskUnitTest {

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testCollectScanNullRequestAppSuccessAndExceptionPropagation() throws Exception {
    ApplicationDetail appDetail = new ApplicationDetail(
        "testPipeline",
        "-SNAPSHOT",
        "desc",
        "{}",
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        new ArtifactSummary("cdap-data-pipeline", "6.12.0", ArtifactScope.SYSTEM),
        null);

    RemoteApplicationDetailFetcher appFetcher = Mockito.mock(RemoteApplicationDetailFetcher.class);
    Mockito.doAnswer(invocation -> {
      @SuppressWarnings("unchecked")
      Consumer<ApplicationDetail> consumer = invocation.getArgument(1);
      consumer.accept(appDetail);
      return null;
    }).when(appFetcher).scan(Mockito.eq("default"), Mockito.any(), Mockito.anyInt());

    RemoteProgramRunRecordsFetcher runsFetcher = Mockito.mock(RemoteProgramRunRecordsFetcher.class);
    RemoteProgramRunRecordFetcher runFetcher = Mockito.mock(RemoteProgramRunRecordFetcher.class);
    RemoteLogsFetcher logsFetcher = Mockito.mock(RemoteLogsFetcher.class);
    RemoteMetricsSystemClient metricsClient = Mockito.mock(RemoteMetricsSystemClient.class);
    SupportBundleJob bundleJob = Mockito.mock(SupportBundleJob.class);

    // 1. Happy path when requestApplication == null
    Mockito.doReturn(Collections.emptyList()).when(runsFetcher)
        .getProgramRuns(Mockito.any(), Mockito.anyLong(), Mockito.anyLong(), Mockito.anyInt());
    File baseDir = tempFolder.newFolder("bundle");
    SupportBundlePipelineInfoTask task = new SupportBundlePipelineInfoTask(
        "uuid-1",
        Collections.singletonList(NamespaceId.DEFAULT),
        null,
        null,
        baseDir,
        appFetcher,
        runsFetcher,
        logsFetcher,
        ProgramType.WORKFLOW,
        "DataPipelineWorkflow",
        metricsClient,
        bundleJob,
        5,
        runFetcher);
    task.collect();

    Assert.assertTrue(new File(new File(baseDir, "testPipeline"), "testPipeline.json").isFile());
    Mockito.verify(bundleJob, Mockito.times(2)).executeTask(
        Mockito.any(), Mockito.eq(baseDir.getPath()), Mockito.anyString(), Mockito.anyString());

    // 2. Checked IOException inside scan callback -> wrapped in RuntimeException
    IOException ioFailure = new IOException("failed fetching runs");
    Mockito.doThrow(ioFailure).when(runsFetcher)
        .getProgramRuns(Mockito.any(), Mockito.anyLong(), Mockito.anyLong(), Mockito.anyInt());
    try {
      task.collect();
      Assert.fail("Expected RuntimeException wrapping IOException");
    } catch (RuntimeException actual) {
      Assert.assertSame(ioFailure, actual.getCause());
    }

    // 3. Unchecked RuntimeException inside scan callback -> propagated directly
    IllegalStateException unchecked = new IllegalStateException("unexpected runtime failure");
    Mockito.doThrow(unchecked).when(runsFetcher)
        .getProgramRuns(Mockito.any(), Mockito.anyLong(), Mockito.anyLong(), Mockito.anyInt());
    try {
      task.collect();
      Assert.fail("Expected IllegalStateException directly");
    } catch (IllegalStateException actual) {
      Assert.assertSame(unchecked, actual);
    }
  }
}
