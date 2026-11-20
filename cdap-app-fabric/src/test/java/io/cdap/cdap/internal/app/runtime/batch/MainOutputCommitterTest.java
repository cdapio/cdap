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

import io.cdap.cdap.api.data.batch.DatasetOutputCommitter;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.app.runtime.batch.dataset.output.ProvidedOutput;
import io.cdap.cdap.messaging.spi.MessagingService;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link MainOutputCommitter}.
 */
public class MainOutputCommitterTest {

  @Test
  public void testCommitJobSuccessAndTxCommitExceptionPropagation() throws Exception {
    JobContext jobContext = createJobContext();
    Transaction tx = new Transaction(100L, 100L, new long[0], new long[0], -1L);
    TransactionSystemClient txClient = Mockito.mock(TransactionSystemClient.class);
    MessagingService messagingService = Mockito.mock(MessagingService.class);
    BasicMapReduceTaskContext<?, ?> taskContext = Mockito.mock(BasicMapReduceTaskContext.class);
    Mockito.when(taskContext.getMessagingService()).thenReturn(messagingService);

    FakeDatasetOutputCommitter dsCommitter = new FakeDatasetOutputCommitter();
    MainOutputCommitter committer = createConfiguredCommitter(
        txClient, tx, taskContext, Collections.singletonList(createProvidedOutput(dsCommitter)));

    committer.commitJob(jobContext);
    Assert.assertEquals(1, dsCommitter.successCount);
    Mockito.verify(taskContext).flushOperations();
    Mockito.verify(txClient).commitOrThrow(tx);
    Mockito.verify(taskContext).postTxCommit();

    // Direct IOException from flushOperations must be propagated unwrapped
    IOException ioFailure = new IOException("flush io failure");
    Mockito.doThrow(ioFailure).when(taskContext).flushOperations();
    try {
      committer.commitJob(jobContext);
      Assert.fail("Expected IOException");
    } catch (IOException thrown) {
      Assert.assertSame(ioFailure, thrown);
    }

    // Unchecked RuntimeException from flushOperations must be propagated unwrapped
    IllegalStateException runtimeFailure = new IllegalStateException("flush runtime failure");
    Mockito.doThrow(runtimeFailure).when(taskContext).flushOperations();
    try {
      committer.commitJob(jobContext);
      Assert.fail("Expected IllegalStateException");
    } catch (RuntimeException thrown) {
      Assert.assertSame(runtimeFailure, thrown);
    }

    // Checked TransactionFailureException from commitOrThrow must be wrapped in RuntimeException
    Mockito.doNothing().when(taskContext).flushOperations();
    TransactionFailureException txFailure = new TransactionFailureException("conflict");
    Mockito.doThrow(txFailure).when(txClient).commitOrThrow(tx);
    try {
      committer.commitJob(jobContext);
      Assert.fail("Expected RuntimeException");
    } catch (RuntimeException thrown) {
      Assert.assertSame(txFailure, thrown.getCause());
    }
  }

  @Test
  public void testOnFinishPropagatesRuntimeExceptionUnwrappedInCommitAndAbort() throws Exception {
    JobContext jobContext = createJobContext();
    Transaction tx = new Transaction(200L, 200L, new long[0], new long[0], -1L);
    TransactionSystemClient txClient = Mockito.mock(TransactionSystemClient.class);
    BasicMapReduceTaskContext<?, ?> taskContext = Mockito.mock(BasicMapReduceTaskContext.class);

    FakeDatasetOutputCommitter dsCommitter = new FakeDatasetOutputCommitter();
    IllegalArgumentException onSuccessFailure = new IllegalArgumentException("onSuccess failed");
    dsCommitter.onSuccessFailure = onSuccessFailure;

    MainOutputCommitter committer = createConfiguredCommitter(
        txClient, tx, taskContext, Collections.singletonList(createProvidedOutput(dsCommitter)));

    try {
      committer.commitJob(jobContext);
      Assert.fail("Expected IllegalArgumentException");
    } catch (RuntimeException thrown) {
      Assert.assertSame(onSuccessFailure, thrown);
    }

    IllegalStateException onFailureException = new IllegalStateException("onFailure failed");
    dsCommitter.onFailureException = onFailureException;
    try {
      committer.abortJob(jobContext, JobStatus.State.FAILED);
      Assert.fail("Expected IllegalStateException");
    } catch (RuntimeException thrown) {
      Assert.assertSame(onFailureException, thrown);
    }
    Mockito.verify(txClient).invalidate(tx.getWritePointer());
  }

  private JobContext createJobContext() {
    JobContext jobContext = Mockito.mock(JobContext.class);
    Mockito.when(jobContext.getConfiguration()).thenReturn(new Configuration());
    return jobContext;
  }

  private ProvidedOutput createProvidedOutput(FakeDatasetOutputCommitter provider) {
    return new ProvidedOutput(
        Output.ofDataset("outDs"),
        provider,
        provider.getOutputFormatClassName(),
        provider.getOutputFormatConfiguration());
  }

  private MainOutputCommitter createConfiguredCommitter(
      TransactionSystemClient txClient,
      Transaction tx,
      BasicMapReduceTaskContext<?, ?> taskContext,
      List<ProvidedOutput> outputs) throws Exception {
    OutputCommitter rootCommitter = Mockito.mock(OutputCommitter.class);
    TaskAttemptContext attemptContext = Mockito.mock(TaskAttemptContext.class);
    MainOutputCommitter committer = new MainOutputCommitter(
        rootCommitter, Collections.emptyMap(), attemptContext);
    setField(committer, "cConf", CConfiguration.create());
    setField(committer, "txClient", txClient);
    setField(committer, "transaction", tx);
    setField(committer, "taskContext", taskContext);
    setField(committer, "outputs", outputs);
    return committer;
  }

  private static void setField(Object target, String name, Object value) throws Exception {
    Field field = MainOutputCommitter.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(target, value);
  }

  private static final class FakeDatasetOutputCommitter
      implements OutputFormatProvider, DatasetOutputCommitter {

    private int successCount;
    private RuntimeException onSuccessFailure;
    private RuntimeException onFailureException;

    @Override
    public String getOutputFormatClassName() {
      return "org.apache.hadoop.mapreduce.lib.output.NullOutputFormat";
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return Collections.emptyMap();
    }

    @Override
    public void onSuccess() {
      successCount++;
      if (onSuccessFailure != null) {
        throw onSuccessFailure;
      }
    }

    @Override
    public void onFailure() {
      if (onFailureException != null) {
        throw onFailureException;
      }
    }
  }
}
