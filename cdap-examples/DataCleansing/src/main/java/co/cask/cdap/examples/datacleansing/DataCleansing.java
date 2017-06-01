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

package co.cask.cdap.examples.datacleansing;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.app.ProgramType;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetProperties;
import co.cask.cdap.api.dataset.lib.Partitioning;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.util.concurrent.TimeUnit;

/**
 * Data Cleansing sample Application.
 */
public class DataCleansing extends AbstractApplication {
  protected static final String NAME = "DataCleansing";
  protected static final String RAW_RECORDS = "rawRecords";
  protected static final String CLEAN_RECORDS = "cleanRecords";
  protected static final String INVALID_RECORDS = "invalidRecords";
  protected static final String CONSUMING_STATE = "consumingState";

  @Override
  public void configure() {
    setName(NAME);
    setDescription("Example data cleansing application");

    // Ingest and retrieve the data using a Service
    addService(new DataCleansingService());

    // Process the records from "rawRecords" partitioned file set using MapReduce
    addMapReduce(new DataCleansingMapReduce());

    addWorkflow(new DataCleansingWorkflow());

    // execute DataCleansingWorkflow when 10 partitions arrive to RAW_RECORDS, while ensuring that there is a
    // 5 minute delay between runs
    schedule(
      buildSchedule("DataSchedule", ProgramType.WORKFLOW, "DataCleansingWorkflow")
      .withDurationSinceLastRun(5, TimeUnit.MINUTES).waitUntilMet()
      .triggerOnPartitions(RAW_RECORDS, 10)
    );

    // Store the state of the incrementally processing MapReduce
    createDataset(CONSUMING_STATE, KeyValueTable.class);

    // Create the "rawRecords" partitioned file set for storing the input records, 
    // configure it to work with MapReduce
    createDataset(RAW_RECORDS, PartitionedFileSet.class, PartitionedFileSetProperties.builder()
      // Properties for partitioning
      .setPartitioning(Partitioning.builder().addLongField("time").build())
      // Properties for file set
      .setInputFormat(TextInputFormat.class)
      .setDescription("Store input records")
      .build());

    createDataset(CLEAN_RECORDS, PartitionedFileSet.class, PartitionedFileSetProperties.builder()
      // Properties for partitioning
      .setPartitioning(Partitioning.builder().addLongField("time").addIntField("zip").build())
      // Properties for file set
      .setOutputFormat(TextOutputFormat.class)
      // Properties for Explore (to create a partitioned Hive table)
      .setEnableExploreOnCreate(true)
      .setExploreFormat("text")
      .setExploreFormatProperty("delimiter", "\n")
      .setExploreSchema("record STRING")
      .setDescription("Store clean records")
      .build());

    createDataset(INVALID_RECORDS, PartitionedFileSet.class, PartitionedFileSetProperties.builder()
      // Properties for partitioning
      .setPartitioning(Partitioning.builder().addLongField("time").build())
      // Properties for file set
      .setOutputFormat(TextOutputFormat.class)
      // Properties for Explore (to create a partitioned Hive table)
      .setEnableExploreOnCreate(true)
      .setExploreFormat("text")
      .setExploreFormatProperty("delimiter", "\n")
      .setExploreSchema("record STRING")
      .setDescription("Store invalid records")
      .build());
  }
}
