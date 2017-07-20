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

import co.cask.cdap.api.dataset.lib.DynamicPartitioner;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.lib.PartitionDetail;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionOutput;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetArguments;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.XSlowTests;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
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
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import static co.cask.cdap.internal.app.runtime.batch.AppWithMapReduceUsingAvroDynamicPartitioner.INPUT_DATASET;
import static co.cask.cdap.internal.app.runtime.batch.AppWithMapReduceUsingAvroDynamicPartitioner.OUTPUT_DATASET;
import static co.cask.cdap.internal.app.runtime.batch.AppWithMapReduceUsingAvroDynamicPartitioner.OUTPUT_PARTITION_KEY;
import static co.cask.cdap.internal.app.runtime.batch.AppWithMapReduceUsingAvroDynamicPartitioner.SCHEMA;

/**
 * This tests that we can use DynamicPartitioner with a PartitionedFileSet using
 * AvroKeyOutputFormat/AvroKeyValueOutputFormat.
 */
@Category(XSlowTests.class)
public class DynamicPartitionerWithAvroTest extends MapReduceRunnerTestBase {
  // ordered, meaning that records with the same partition are adjacent to each other (read: not necessarily sorted)
  // This is necessary for testing multi vs single writer
  private static final List<? extends GenericRecord> ORDERED_RECORDS =
    ImmutableList.of(createRecord("bob", 95111),
                     createRecord("sally", 98123),
                     createRecord("jane", 84125),
                     createRecord("john", 84125));

  @Test
  public void testMultiWriter() throws Exception {
    runDynamicPartitionerMR(ORDERED_RECORDS, true, true);
  }

  @Test
  public void testSingleWriter() throws Exception {
    runDynamicPartitionerMR(ORDERED_RECORDS, false, true);
  }

  @Test
  public void testSingleWriterWithUnorderedData() throws Exception {
    List<? extends GenericRecord> records =
      ImmutableList.of(createRecord("bob", 95111),
                       createRecord("jane", 84125),
                       createRecord("sally", 98123),
                       createRecord("john", 84125));
    // the input data is not ordered by output partition and its limiting to a single writer,
    // so we expect this job to fail
    runDynamicPartitionerMR(records, false, false);
  }

  @Test
  public void testPartitionAppend() throws Exception {
    runDynamicPartitionerMR(ORDERED_RECORDS, true, true,
                            DynamicPartitioner.PartitionWriteOption.CREATE_OR_APPEND, true);
  }

  @Test
  public void testPartitionAppendWhenNotConfigured() throws Exception {
    // partition will exist beforehand, but the append option is not configured; hence the job is expected to fail
    runDynamicPartitionerMR(ORDERED_RECORDS, true, true, DynamicPartitioner.PartitionWriteOption.CREATE, false);
  }

  @Test
  public void testPartitionOverwrite() throws Exception {
    runDynamicPartitionerMR(ORDERED_RECORDS, true, true,
                            DynamicPartitioner.PartitionWriteOption.CREATE_OR_OVERWRITE, true);
  }

  private void writeFile(PartitionedFileSet pfs, PartitionKey key) throws IOException {
    PartitionOutput partitionOutput = pfs.getPartitionOutput(key);
    partitionOutput.getLocation().mkdirs();
    partitionOutput.getLocation().append("file").createNew();
    partitionOutput.setMetadata(ImmutableMap.of("file", "file"));
    partitionOutput.addPartition();
  }

  private void runDynamicPartitionerMR(final List<? extends GenericRecord> records,
                                       boolean allowConcurrentWriters,
                                       boolean expectedStatus) throws Exception {
    runDynamicPartitionerMR(records, allowConcurrentWriters, false, null, expectedStatus);
  }

  private void runDynamicPartitionerMR(final List<? extends GenericRecord> records,
                                       boolean allowConcurrentWriters,
                                       final boolean precreatePartitions,
                                       @Nullable final DynamicPartitioner.PartitionWriteOption partitionWriteOption,
                                       boolean expectedStatus) throws Exception {
    ApplicationWithPrograms app = deployApp(AppWithMapReduceUsingAvroDynamicPartitioner.class);

    final long now = System.currentTimeMillis();
    final Multimap<PartitionKey, GenericRecord> keyToRecordsMap = groupByPartitionKey(records, now);

    // write values to the input kvTable
    final KeyValueTable kvTable = datasetCache.getDataset(INPUT_DATASET);
    Transactions.createTransactionExecutor(txExecutorFactory, kvTable).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          // the keys are not used; it matters that they're unique though
          for (int i = 0; i < records.size(); i++) {
            kvTable.write(Integer.toString(i), records.get(i).toString());
          }
        }
      });

    final PartitionedFileSet pfs = datasetCache.getDataset(OUTPUT_DATASET);

    if (precreatePartitions) {
      Transactions.createTransactionExecutor(txExecutorFactory, (TransactionAware) pfs).execute(
        new TransactionExecutor.Subroutine() {
          @Override
          public void apply() throws IOException {
            writeFile(pfs, createKey(now, 95111));
            writeFile(pfs, createKey(now, 98123));
            writeFile(pfs, createKey(now, 84125));
          }
        });
    }

    String allowConcurrencyKey =
      "dataset." + OUTPUT_DATASET + "." + PartitionedFileSetArguments.DYNAMIC_PARTITIONER_ALLOW_CONCURRENCY;
    // run the partition writer m/r with this output partition time
    Map<String, String> arguments = new HashMap<>();
    arguments.put(OUTPUT_PARTITION_KEY, Long.toString(now));
    arguments.put(allowConcurrencyKey, Boolean.toString(allowConcurrentWriters));
    if (partitionWriteOption != null) {
      arguments.put("partitionWriteOption", partitionWriteOption.name());
    }
    long startTime = System.currentTimeMillis();
    boolean status = runProgram(app, AppWithMapReduceUsingAvroDynamicPartitioner.DynamicPartitioningMapReduce.class,
                                new BasicArguments(arguments));
    Assert.assertEquals(expectedStatus, status);

    if (!expectedStatus) {
      // if we expect the program to fail, no need to check the output data for expected results
      return;
    }

    // Verify notifications
    List<Notification> notifications = getDataNotifications(startTime);
    Assert.assertEquals(1, notifications.size());
    Assert.assertEquals(NamespaceId.DEFAULT.dataset(OUTPUT_DATASET),
                        DatasetId.fromString(notifications.get(0).getProperties().get("datasetId")));

    // this should have created a partition in the pfs
    final Location pfsBaseLocation = pfs.getEmbeddedFileSet().getBaseLocation();

    Transactions.createTransactionExecutor(txExecutorFactory, (TransactionAware) pfs).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws IOException {
          Map<PartitionKey, PartitionDetail> partitions = new HashMap<>();
          for (PartitionDetail partition : pfs.getPartitions(null)) {
            partitions.put(partition.getPartitionKey(), partition);
            // check that the mapreduce wrote the output partition metadata to all the output partitions
            Assert.assertEquals(getExpectedMetadata(precreatePartitions, partitionWriteOption),
                                partition.getMetadata().asMap());
            // if files were precreated, and the option is to append, expect the empty file to exist
            // if partition write option is configured to overwrite, then the file is expected to not exist
            Location preexistingFile = partition.getLocation().append("file");
            if (precreatePartitions &&
              partitionWriteOption == DynamicPartitioner.PartitionWriteOption.CREATE_OR_APPEND) {
              Assert.assertTrue(preexistingFile.exists());
              try (InputStream inputStream = preexistingFile.getInputStream()) {
                Assert.assertEquals(-1, inputStream.read());
              }
            } else {
              Assert.assertFalse(preexistingFile.exists());
            }
          }
          Assert.assertEquals(3, partitions.size());

          Assert.assertEquals(keyToRecordsMap.keySet(), partitions.keySet());

          // Check relative paths of the partitions. Also check that their location = pfs baseLocation + relativePath
          for (Map.Entry<PartitionKey, PartitionDetail> partitionKeyEntry : partitions.entrySet()) {
            PartitionDetail partitionDetail = partitionKeyEntry.getValue();
            String relativePath = partitionDetail.getRelativePath();
            int zip = (int) partitionKeyEntry.getKey().getField("zip");
            Assert.assertEquals(Long.toString(now) + Path.SEPARATOR + zip,
                                relativePath);

            Assert.assertEquals(pfsBaseLocation.append(relativePath), partitionDetail.getLocation());
          }

          for (Map.Entry<PartitionKey, Collection<GenericRecord>> keyToRecordsEntry :
            keyToRecordsMap.asMap().entrySet()) {

            Set<GenericRecord> genericRecords = new HashSet<>(keyToRecordsEntry.getValue());
            Assert.assertEquals(genericRecords, readOutput(partitions.get(keyToRecordsEntry.getKey()).getLocation()));
          }
        }
      });
  }

  private Map<String, String> getExpectedMetadata(boolean precreatePartitions,
                                                  DynamicPartitioner.PartitionWriteOption writeOption) {
    Map<String, String> expectedMetadata =
      new HashMap<>(AppWithMapReduceUsingAvroDynamicPartitioner.DynamicPartitioningMapReduce.METADATA);
    // if we're appending to a pre-existing partition, expect the previous partition metadata 'file=file' to also exist
    // overwrite option will overwrite the metadata
    if (precreatePartitions && writeOption == DynamicPartitioner.PartitionWriteOption.CREATE_OR_APPEND) {
      expectedMetadata.put("file", "file");
    }
    return expectedMetadata;
  }

  private Multimap<PartitionKey, GenericRecord> groupByPartitionKey(List<? extends GenericRecord> records, long now) {
    HashMultimap<PartitionKey, GenericRecord> groupedByPartitionKey = HashMultimap.create();
    for (GenericRecord record : records) {
      groupedByPartitionKey.put(createKey(now, (int) record.get("zip")), record);
    }
    return groupedByPartitionKey;
  }

  private PartitionKey createKey(long time, int zip) {
    return PartitionKey.builder().addLongField("time", time).addIntField("zip", zip).build();
  }

  private Set<GenericRecord> readOutput(Location location) throws IOException {
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(SCHEMA);
    Set<GenericRecord> records = new HashSet<>();
    for (Location file : location.list()) {
      if (file.getName().endsWith(".avro")) {
        DataFileStream<GenericRecord> fileStream = new DataFileStream<>(file.getInputStream(), datumReader);
        Iterables.addAll(records, fileStream);
        fileStream.close();
      }
    }
    return records;
  }

  private static GenericData.Record createRecord(String name, int zip) {
    GenericData.Record record = new GenericData.Record(SCHEMA);
    record.put("name", name);
    record.put("zip", zip);
    return record;
  }
}
