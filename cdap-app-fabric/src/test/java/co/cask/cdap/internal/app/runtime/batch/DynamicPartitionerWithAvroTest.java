/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.batch;

import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.lib.PartitionDetail;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.test.XSlowTests;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.fs.Path;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionExecutor;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static co.cask.cdap.internal.app.runtime.batch.AppWithMapReduceUsingAvroDynamicPartitioner.INPUT_DATASET;
import static co.cask.cdap.internal.app.runtime.batch.AppWithMapReduceUsingAvroDynamicPartitioner.OUTPUT_DATASET;
import static co.cask.cdap.internal.app.runtime.batch.AppWithMapReduceUsingAvroDynamicPartitioner.OUTPUT_PARTITION_KEY;
import static co.cask.cdap.internal.app.runtime.batch.AppWithMapReduceUsingAvroDynamicPartitioner.SCHEMA;

@Category(XSlowTests.class)
/**
 * This tests that we can use DynamicPartitioner with a PartitionedFileSet using
 * AvroKeyOutputFormat/AvroKeyValueOutputFormat.
 */
public class DynamicPartitionerWithAvroTest extends MapReduceRunnerTestBase {

  private GenericData.Record createRecord(String name, int zip) {
    GenericData.Record record = new GenericData.Record(SCHEMA);
    record.put("name", name);
    record.put("zip", zip);
    return record;
  }

  @Test
  public void testDynamicPartitionerWithAvro() throws Exception {
    ApplicationWithPrograms app = deployApp(AppWithMapReduceUsingAvroDynamicPartitioner.class);

    final GenericRecord record1 = createRecord("bob", 95111);
    final GenericRecord record2 = createRecord("sally", 98123);
    final GenericRecord record3 = createRecord("jane", 84125);
    final GenericRecord record4 = createRecord("john", 84125);

    final long now = System.currentTimeMillis();
    // the four records above will fall into the partitions defined by the following three keys
    // we have four records, but only 3 have unique zip codes (which is what we partition by)
    final PartitionKey key1 = PartitionKey.builder().addLongField("time", now).addIntField("zip", 95111).build();
    final PartitionKey key2 = PartitionKey.builder().addLongField("time", now).addIntField("zip", 98123).build();
    final PartitionKey key3 = PartitionKey.builder().addLongField("time", now).addIntField("zip", 84125).build();
    final Set<PartitionKey> expectedKeys = ImmutableSet.of(key1, key2, key3);

    // write values to the input kvTable
    final KeyValueTable kvTable = datasetCache.getDataset(INPUT_DATASET);
    Transactions.createTransactionExecutor(txExecutorFactory, kvTable).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          // the keys are not used; it matters that they're unique though
          kvTable.write("1", record1.toString());
          kvTable.write("2", record2.toString());
          kvTable.write("3", record3.toString());
          kvTable.write("4", record4.toString());
        }
      });


    // run the partition writer m/r with this output partition time
    runProgram(app, AppWithMapReduceUsingAvroDynamicPartitioner.DynamicPartitioningMapReduce.class,
               new BasicArguments(ImmutableMap.of(OUTPUT_PARTITION_KEY, Long.toString(now))));

    // this should have created a partition in the pfs
    final PartitionedFileSet pfs = datasetCache.getDataset(OUTPUT_DATASET);
    final Location pfsBaseLocation = pfs.getEmbeddedFileSet().getBaseLocation();

    Transactions.createTransactionExecutor(txExecutorFactory, (TransactionAware) pfs).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws IOException {
          Map<PartitionKey, PartitionDetail> partitions = new HashMap<>();
          for (PartitionDetail partition : pfs.getPartitions(null)) {
            partitions.put(partition.getPartitionKey(), partition);
            // check that the mapreduce wrote the output partition metadata to all the output partitions
            Assert.assertEquals(AppWithMapReduceUsingAvroDynamicPartitioner.DynamicPartitioningMapReduce.METADATA,
                                partition.getMetadata().asMap());
          }
          Assert.assertEquals(3, partitions.size());

          Assert.assertEquals(expectedKeys, partitions.keySet());

          // Check the relative path of one partition. Also check that its location = pfs base location + relativePath
          PartitionDetail partition1 = partitions.get(key1);
          String relativePath = partition1.getRelativePath();
          Assert.assertEquals(Long.toString(now) + Path.SEPARATOR + Integer.toString((int) key1.getField("zip")),
                              relativePath);

          Assert.assertEquals(pfsBaseLocation.append(relativePath), partition1.getLocation());

          Assert.assertEquals(ImmutableList.of(record1), readOutput(partition1.getLocation()));
          Assert.assertEquals(ImmutableList.of(record2), readOutput(partitions.get(key2).getLocation()));
          Assert.assertEquals(ImmutableList.of(record3, record4), readOutput(partitions.get(key3).getLocation()));
        }
      });
  }

  private List<GenericRecord> readOutput(Location location) throws IOException {
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(SCHEMA);
    List<GenericRecord> records = new ArrayList<>();
    for (Location file : location.list()) {
      if (file.getName().endsWith(".avro")) {
        DataFileStream<GenericRecord> fileStream = new DataFileStream<>(file.getInputStream(), datumReader);
        Iterables.addAll(records, fileStream);
        fileStream.close();
      }
    }
    return records;
  }
}
