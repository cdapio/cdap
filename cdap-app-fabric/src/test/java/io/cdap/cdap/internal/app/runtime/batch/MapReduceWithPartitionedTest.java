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

package io.cdap.cdap.internal.app.runtime.batch;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.common.RuntimeArguments;
import io.cdap.cdap.api.common.Scope;
import io.cdap.cdap.api.dataset.lib.Partition;
import io.cdap.cdap.api.dataset.lib.PartitionFilter;
import io.cdap.cdap.api.dataset.lib.PartitionKey;
import io.cdap.cdap.api.dataset.lib.PartitionedFileSet;
import io.cdap.cdap.api.dataset.lib.PartitionedFileSetArguments;
import io.cdap.cdap.api.dataset.lib.TimePartitionDetail;
import io.cdap.cdap.api.dataset.lib.TimePartitionedFileSet;
import io.cdap.cdap.api.dataset.lib.TimePartitionedFileSetArguments;
import io.cdap.cdap.api.dataset.table.Row;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.data2.transaction.Transactions;
import io.cdap.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.test.XSlowTests;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionExecutor;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.text.DateFormat;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.cdap.cdap.internal.app.runtime.batch.AppWithPartitionedFileSet.PARTITIONED;
import static io.cdap.cdap.internal.app.runtime.batch.AppWithTimePartitionedFileSet.TIME_PARTITIONED;

/**
 * This tests that we can read and write time-partitioned file sets with map/reduce, using the partition
 * time to specify input and output partitions. It does not test that the dataset is queryable with Hive,
 * or that its partitions are registered correctly in the Hive meta store. That is because here in the
 * app-fabric test
 */
@Category(XSlowTests.class)
public class MapReduceWithPartitionedTest extends MapReduceRunnerTestBase {

  static final DateFormat DATE_FORMAT = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT, Locale.US);


  @Test
  public void testTimePartitionedWithMR() throws Exception {

    final ApplicationWithPrograms app = deployApp(AppWithTimePartitionedFileSet.class);

    // write a value to the input table
    final Table table = datasetCache.getDataset(AppWithTimePartitionedFileSet.INPUT);
    Transactions.createTransactionExecutor(txExecutorFactory, (TransactionAware) table).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          table.put(Bytes.toBytes("x"), AppWithTimePartitionedFileSet.ONLY_COLUMN, Bytes.toBytes("1"));
        }
      });

    final long time = DATE_FORMAT.parse("1/15/15 11:15 am").getTime();
    final long time5 = time + TimeUnit.MINUTES.toMillis(5);

    // run the partition writer m/r with this output partition time
    Map<String, String> runtimeArguments = Maps.newHashMap();
    Map<String, String> outputArgs = Maps.newHashMap();
    TimePartitionedFileSetArguments.setOutputPartitionTime(outputArgs, time);
    final ImmutableMap<String, String> assignedMetadata = ImmutableMap.of("region", "13",
                                                                          "data.source.name", "input",
                                                                          "data.source.type", "table");
    TimePartitionedFileSetArguments.setOutputPartitionMetadata(outputArgs, assignedMetadata);
    runtimeArguments.putAll(RuntimeArguments.addScope(Scope.DATASET, TIME_PARTITIONED, outputArgs));
    Assert.assertTrue(
      runProgram(app, AppWithTimePartitionedFileSet.PartitionWriter.class, new BasicArguments(runtimeArguments)));

    // this should have created a partition in the tpfs
    final TimePartitionedFileSet tpfs = datasetCache.getDataset(TIME_PARTITIONED);
    Transactions.createTransactionExecutor(txExecutorFactory, (TransactionAware) tpfs).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          TimePartitionDetail partition = tpfs.getPartitionByTime(time);
          Assert.assertNotNull(partition);
          String path = partition.getRelativePath();
          Assert.assertNotNull(path);
          Assert.assertTrue(path.contains("2015-01-15/11-15"));
          Assert.assertEquals(assignedMetadata, partition.getMetadata().asMap());
        }
      });

    // delete the data in the input table and write a new row
    Transactions.createTransactionExecutor(txExecutorFactory, (TransactionAware) table).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          table.delete(Bytes.toBytes("x"));
          table.put(Bytes.toBytes("y"), AppWithTimePartitionedFileSet.ONLY_COLUMN, Bytes.toBytes("2"));
        }
      });

    // now run the m/r again with a new partition time, say 5 minutes later
    TimePartitionedFileSetArguments.setOutputPartitionTime(outputArgs, time5);
    runtimeArguments.putAll(RuntimeArguments.addScope(Scope.DATASET, TIME_PARTITIONED, outputArgs));
    // make the mapreduce add the partition in destroy, to validate that this does not fail the job
    runtimeArguments.put(AppWithTimePartitionedFileSet.COMPAT_ADD_PARTITION, "true");
    Assert.assertTrue(
      runProgram(app, AppWithTimePartitionedFileSet.PartitionWriter.class, new BasicArguments(runtimeArguments)));

    // this should have created a partition in the tpfs
    Transactions.createTransactionExecutor(txExecutorFactory, (TransactionAware) tpfs).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          Partition partition = tpfs.getPartitionByTime(time5);
          Assert.assertNotNull(partition);
          String path = partition.getRelativePath();
          Assert.assertNotNull(path);
          Assert.assertTrue(path.contains("2015-01-15/11-20"));
        }
      });

    // now run a map/reduce that reads all the partitions
    runtimeArguments = Maps.newHashMap();
    Map<String, String> inputArgs = Maps.newHashMap();
    TimePartitionedFileSetArguments.setInputStartTime(inputArgs, time - TimeUnit.MINUTES.toMillis(5));
    TimePartitionedFileSetArguments.setInputEndTime(inputArgs, time5 + TimeUnit.MINUTES.toMillis(5));
    runtimeArguments.putAll(RuntimeArguments.addScope(Scope.DATASET, TIME_PARTITIONED, inputArgs));
    runtimeArguments.put(AppWithTimePartitionedFileSet.ROW_TO_WRITE, "a");
    Assert.assertTrue(
      runProgram(app, AppWithTimePartitionedFileSet.PartitionReader.class, new BasicArguments(runtimeArguments)));

    // this should have read both partitions - and written both x and y to row a
    final Table output = datasetCache.getDataset(AppWithTimePartitionedFileSet.OUTPUT);
    Transactions.createTransactionExecutor(txExecutorFactory, (TransactionAware) output).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          Row row = output.get(Bytes.toBytes("a"));
          Assert.assertEquals("1", row.getString("x"));
          Assert.assertEquals("2", row.getString("y"));
        }
      });

    // now run a map/reduce that reads a range of the partitions, namely the first one
    TimePartitionedFileSetArguments.setInputStartTime(inputArgs, time - TimeUnit.MINUTES.toMillis(5));
    TimePartitionedFileSetArguments.setInputEndTime(inputArgs, time + TimeUnit.MINUTES.toMillis(2));
    runtimeArguments.putAll(RuntimeArguments.addScope(Scope.DATASET, TIME_PARTITIONED, inputArgs));
    runtimeArguments.put(AppWithTimePartitionedFileSet.ROW_TO_WRITE, "b");
    Assert.assertTrue(
      runProgram(app, AppWithTimePartitionedFileSet.PartitionReader.class, new BasicArguments(runtimeArguments)));

    // this should have read the first partition only - and written only x to row b
    Transactions.createTransactionExecutor(txExecutorFactory, (TransactionAware) output).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          Row row = output.get(Bytes.toBytes("b"));
          Assert.assertEquals("1", row.getString("x"));
          Assert.assertNull(row.get("y"));
        }
      });

    // now run a map/reduce that reads no partitions (because the range matches nothing)
    TimePartitionedFileSetArguments.setInputStartTime(inputArgs, time - TimeUnit.MINUTES.toMillis(10));
    TimePartitionedFileSetArguments.setInputEndTime(inputArgs, time - TimeUnit.MINUTES.toMillis(9));
    runtimeArguments.putAll(RuntimeArguments.addScope(Scope.DATASET, TIME_PARTITIONED, inputArgs));
    runtimeArguments.put(AppWithTimePartitionedFileSet.ROW_TO_WRITE, "n");
    Assert.assertTrue(
      runProgram(app, AppWithTimePartitionedFileSet.PartitionReader.class, new BasicArguments(runtimeArguments)));

    // this should have read no partitions - and written nothing to row n
    Transactions.createTransactionExecutor(txExecutorFactory, (TransactionAware) output).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          Row row = output.get(Bytes.toBytes("n"));
          Assert.assertTrue(row.isEmpty());
        }
      });
  }

  @Test
  public void testPartitionedFileSetWithMR() throws Exception {
    testPartitionedFileSetWithMR(false);
  }

  @Test
  public void testPartitionedFileSetWithMRWithCombineFileInputFormat() throws Exception {
    testPartitionedFileSetWithMR(true);
  }

  private void testPartitionedFileSetWithMR(boolean useCombineFileInputFormat) throws Exception {
    ApplicationWithPrograms app = deployApp(AppWithPartitionedFileSet.class,
                                            new AppWithPartitionedFileSet.AppConfig(useCombineFileInputFormat));
    // write a value to the input table
    final Table table = datasetCache.getDataset(AppWithPartitionedFileSet.INPUT);
    Transactions.createTransactionExecutor(txExecutorFactory, (TransactionAware) table).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          table.put(Bytes.toBytes("x"), AppWithPartitionedFileSet.ONLY_COLUMN, Bytes.toBytes("1"));
        }
      });

    // a partition key for the map/reduce output
    final PartitionKey keyX = PartitionKey.builder()
      .addStringField("type", "x")
      .addLongField("time", 150000L)
      .build();

    // run the partition writer m/r with this output partition time
    Map<String, String> runtimeArguments = Maps.newHashMap();
    Map<String, String> outputArgs = Maps.newHashMap();
    PartitionedFileSetArguments.setOutputPartitionKey(outputArgs, keyX);
    runtimeArguments.putAll(RuntimeArguments.addScope(Scope.DATASET, PARTITIONED, outputArgs));
    Assert.assertTrue(
      runProgram(app, AppWithPartitionedFileSet.PartitionWriter.class, new BasicArguments(runtimeArguments)));

    // this should have created a partition in the tpfs
    final PartitionedFileSet dataset = datasetCache.getDataset(PARTITIONED);
    Transactions.createTransactionExecutor(txExecutorFactory, (TransactionAware) dataset).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          Partition partition = dataset.getPartition(keyX);
          Assert.assertNotNull(partition);
          String path = partition.getRelativePath();
          Assert.assertTrue(path.contains("x"));
          Assert.assertTrue(path.contains("150000"));
        }
      });

    // delete the data in the input table and write a new row
    Transactions.createTransactionExecutor(txExecutorFactory, (TransactionAware) table).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          table.delete(Bytes.toBytes("x"));
          table.put(Bytes.toBytes("y"), AppWithPartitionedFileSet.ONLY_COLUMN, Bytes.toBytes("2"));
        }
      });

    // a new partition key for the next map/reduce
    final PartitionKey keyY = PartitionKey.builder()
      .addStringField("type", "y")
      .addLongField("time", 200000L)
      .build();

    // now run the m/r again with a new partition time, say 5 minutes later
    PartitionedFileSetArguments.setOutputPartitionKey(outputArgs, keyY);
    runtimeArguments.putAll(RuntimeArguments.addScope(Scope.DATASET, PARTITIONED, outputArgs));
    Assert.assertTrue(
      runProgram(app, AppWithPartitionedFileSet.PartitionWriter.class, new BasicArguments(runtimeArguments)));

    // this should have created a partition in the tpfs
    Transactions.createTransactionExecutor(txExecutorFactory, (TransactionAware) dataset).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          Partition partition = dataset.getPartition(keyY);
          Assert.assertNotNull(partition);
          String path = partition.getRelativePath();
          Assert.assertNotNull(path);
          Assert.assertTrue(path.contains("y"));
          Assert.assertTrue(path.contains("200000"));
        }
      });

    // a partition filter that matches the outputs of both map/reduces
    PartitionFilter filterXY = PartitionFilter.builder()
      .addRangeCondition("type", "x", "z")
      .build();

    // now run a map/reduce that reads all the partitions
    runtimeArguments = Maps.newHashMap();
    Map<String, String> inputArgs = Maps.newHashMap();
    PartitionedFileSetArguments.setInputPartitionFilter(inputArgs, filterXY);
    runtimeArguments.putAll(RuntimeArguments.addScope(Scope.DATASET, PARTITIONED, inputArgs));
    runtimeArguments.put(AppWithPartitionedFileSet.ROW_TO_WRITE, "a");
    Assert.assertTrue(
      runProgram(app, AppWithPartitionedFileSet.PartitionReader.class, new BasicArguments(runtimeArguments)));

    // this should have read both partitions - and written both x and y to row a
    final Table output = datasetCache.getDataset(AppWithPartitionedFileSet.OUTPUT);
    Transactions.createTransactionExecutor(txExecutorFactory, (TransactionAware) output).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          Row row = output.get(Bytes.toBytes("a"));
          Assert.assertEquals("1", row.getString("x"));
          Assert.assertEquals("{type=x, time=150000}", row.getString("x_key"));
          Assert.assertEquals("2", row.getString("y"));
          Assert.assertEquals("{type=y, time=200000}", row.getString("y_key"));
        }
      });

    // a partition filter that matches the output key of the first map/reduce
    PartitionFilter filterX = PartitionFilter.builder()
      .addValueCondition("type", "x")
      .addRangeCondition("time", null, 160000L)
      .build();

    // now run a map/reduce that reads a range of the partitions, namely the first one
    inputArgs.clear();
    PartitionedFileSetArguments.setInputPartitionFilter(inputArgs, filterX);
    runtimeArguments.putAll(RuntimeArguments.addScope(Scope.DATASET, PARTITIONED, inputArgs));
    runtimeArguments.put(AppWithPartitionedFileSet.ROW_TO_WRITE, "b");
    Assert.assertTrue(
      runProgram(app, AppWithPartitionedFileSet.PartitionReader.class, new BasicArguments(runtimeArguments)));

    // this should have read the first partition only - and written only x to row b
    Transactions.createTransactionExecutor(txExecutorFactory, (TransactionAware) output).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          Row row = output.get(Bytes.toBytes("b"));
          Assert.assertEquals("1", row.getString("x"));
          Assert.assertEquals("{type=x, time=150000}", row.getString("x_key"));
          Assert.assertNull(row.get("y"));
          Assert.assertNull(row.get("y_key"));
        }
      });

    // a partition filter that matches no key
    PartitionFilter filterMT = PartitionFilter.builder()
      .addValueCondition("type", "nosuchthing")
      .build();

    // now run a map/reduce that reads an empty range of partitions (the filter matches nothing)
    inputArgs.clear();
    PartitionedFileSetArguments.setInputPartitionFilter(inputArgs, filterMT);
    runtimeArguments.putAll(RuntimeArguments.addScope(Scope.DATASET, PARTITIONED, inputArgs));
    runtimeArguments.put(AppWithPartitionedFileSet.ROW_TO_WRITE, "n");
    Assert.assertTrue(
      runProgram(app, AppWithPartitionedFileSet.PartitionReader.class, new BasicArguments(runtimeArguments)));

    // this should have read no partitions - and written nothing to row n
    Transactions.createTransactionExecutor(txExecutorFactory, (TransactionAware) output).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          Row row = output.get(Bytes.toBytes("n"));
          Assert.assertTrue(row.isEmpty());
        }
      });
  }
}
