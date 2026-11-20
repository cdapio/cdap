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

package io.cdap.cdap.internal.app.runtime.schedule.queue;

import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramSchedule;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.TimeTrigger;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ScheduleId;
import io.cdap.cdap.proto.id.WorkflowId;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.store.StoreDefinition;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Deterministic unit tests for {@link JobQueueTable#getPartition(ScheduleId)} and partition pagination.
 */
public class JobQueueTablePartitionTest {

  @Test
  public void testGetPartitionDeterministicAcrossAsciiAndUnicode() {
    ScheduleId ascii1 = new ScheduleId("default", "MyApp", "DataPipelineSchedule");
    ScheduleId ascii2 = new ScheduleId("prod_ns", "OrderETL", "HourlyTrigger");
    ScheduleId latinUnicode = new ScheduleId("default", "app", "sched_üñîçødé");
    ScheduleId cjkUnicode = new ScheduleId("analytics", "pipeline", "日次スケジュール");

    ScheduleId unicodeNamespace = Mockito.mock(ScheduleId.class);
    Mockito.when(unicodeNamespace.getNamespace()).thenReturn("ns_üñîçødé");
    Mockito.when(unicodeNamespace.getApplication()).thenReturn("app");
    Mockito.when(unicodeNamespace.getSchedule()).thenReturn("sched");

    JobQueueTable table8 = createTable(8);
    Assert.assertEquals(7, table8.getPartition(ascii1));
    Assert.assertEquals(6, table8.getPartition(ascii2));
    Assert.assertEquals(4, table8.getPartition(latinUnicode));
    Assert.assertEquals(3, table8.getPartition(cjkUnicode));
    Assert.assertEquals(4, table8.getPartition(unicodeNamespace));

    JobQueueTable table16 = createTable(16);
    Assert.assertEquals(15, table16.getPartition(ascii1));
    Assert.assertEquals(6, table16.getPartition(ascii2));
    Assert.assertEquals(4, table16.getPartition(latinUnicode));
    Assert.assertEquals(11, table16.getPartition(cjkUnicode));
    Assert.assertEquals(4, table16.getPartition(unicodeNamespace));

    JobQueueTable table17 = createTable(17);
    Assert.assertEquals(1, table17.getPartition(ascii1));
    Assert.assertEquals(4, table17.getPartition(ascii2));
    Assert.assertEquals(12, table17.getPartition(latinUnicode));
    Assert.assertEquals(15, table17.getPartition(cjkUnicode));
    Assert.assertEquals(11, table17.getPartition(unicodeNamespace));
  }

  @Test
  public void testGetJobsPaginationAndPartitionCheck() throws Exception {
    StructuredTable structuredTable = Mockito.mock(StructuredTable.class);
    Mockito.when(structuredTable.scan(Mockito.any(Range.class), Mockito.eq(Integer.MAX_VALUE)))
        .thenAnswer(invocation -> emptyCloseableIterator());

    JobQueueTable jobQueue = new JobQueueTable(
        structuredTable, Mockito.mock(AppMetadataStore.class), 16);

    WorkflowId workflowId = new ApplicationId("default", "MyApp").workflow("wf1");
    ProgramSchedule schedule = new ProgramSchedule(
        "DataPipelineSchedule", "desc", workflowId,
        Collections.emptyMap(), new TimeTrigger("0 * * * *"), Collections.emptyList());
    Job lastJob = new SimpleJob(
        schedule, 3, 1000L, Collections.emptyList(), Job.State.PENDING_TRIGGER, 500L);

    // Partition for ("default", "MyApp", "DataPipelineSchedule") with 16 partitions is 15.
    int expectedPartition = 15;
    Assert.assertEquals(expectedPartition, jobQueue.getPartition(schedule.getScheduleId()));

    // Initial scan without cursor uses INCLUSIVE begin bound on partition field.
    try (CloseableIterator<Job> iter = jobQueue.getJobs(expectedPartition, null)) {
      Assert.assertFalse(iter.hasNext());
    }

    // Resuming scan with lastJobProcessed in matching partition uses EXCLUSIVE begin bound.
    try (CloseableIterator<Job> iter = jobQueue.getJobs(expectedPartition, lastJob)) {
      Assert.assertFalse(iter.hasNext());
    }

    ArgumentCaptor<Range> rangeCaptor = ArgumentCaptor.forClass(Range.class);
    Mockito.verify(structuredTable, Mockito.times(2))
        .scan(rangeCaptor.capture(), Mockito.eq(Integer.MAX_VALUE));
    List<Range> capturedRanges = rangeCaptor.getAllValues();

    Assert.assertEquals(Range.Bound.INCLUSIVE, capturedRanges.get(0).getBeginBound());
    Assert.assertEquals(Range.Bound.EXCLUSIVE, capturedRanges.get(1).getBeginBound());
    List<Field<?>> beginFields = new ArrayList<>(capturedRanges.get(1).getBegin());
    Assert.assertEquals(
        Fields.intField(StoreDefinition.JobQueueStore.PARTITION_ID, expectedPartition),
        beginFields.get(0));
    Assert.assertEquals(
        Fields.intField(StoreDefinition.JobQueueStore.GENERATION_ID, 3),
        beginFields.get(2));

    // Passing a job from another partition must fail the Preconditions check.
    int wrongPartition = 6;
    try {
      jobQueue.getJobs(wrongPartition, lastJob);
      Assert.fail("Expected IllegalArgumentException for mismatched partition");
    } catch (IllegalArgumentException expected) {
      Assert.assertTrue(expected.getMessage().contains("Job is not from partition"));
    }
  }

  private JobQueueTable createTable(int numPartitions) {
    return new JobQueueTable(
        Mockito.mock(StructuredTable.class),
        Mockito.mock(AppMetadataStore.class),
        numPartitions);
  }

  private CloseableIterator<StructuredRow> emptyCloseableIterator() {
    return CloseableIterator.empty();
  }
}
