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

package co.cask.cdap.examples.streamconversion;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.schedule.Schedules;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;

/**
 * An application that illustrates the use of time-partitioned file sets by the example of
 * periodic stream conversion.
 */
public class StreamConversionApp extends AbstractApplication {

  static final String SCHEMA_STRING = Schema.recordOf(
    "streamEvent",
    Schema.Field.of("time", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("body", Schema.of(Schema.Type.STRING))).toString();

  @Override
  public void configure() {
    addStream(new Stream("events"));
    addMapReduce(new StreamConversionMapReduce());
    addWorkflow(new StreamConversionWorkflow());
    scheduleWorkflow(Schedules.createTimeSchedule("every5min", "runs every 5 minutes", "*/5 * * * *"),
                     "StreamConversionWorkflow");

    // create the time-partitioned file set, configure it to work with MapReduce and with Explore
    createDataset("converted", TimePartitionedFileSet.class, FileSetProperties.builder()
      // properties for file set
      .setBasePath("/converted")
      .setInputFormat(AvroKeyInputFormat.class)
      .setOutputFormat(AvroKeyOutputFormat.class)
      .setOutputProperty("schema", SCHEMA_STRING)
        // properties for explore (to create a partitioned hive table)
      .setEnableExploreOnCreate(true)
      .setSerDe("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
      .setExploreInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
      .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
      .setTableProperty("avro.schema.literal", SCHEMA_STRING)
      .build());
  }
}
