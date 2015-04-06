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

package co.cask.cdap.templates.etl.batch.sinks;

import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSetArguments;
import co.cask.cdap.templates.etl.api.Property;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.batch.BatchSink;
import co.cask.cdap.templates.etl.api.batch.BatchSinkContext;
import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;

import java.util.Map;

/**
 * A {@link BatchSink} to write Avro record to {@link TimePartitionedFileSet}
 */
public class TimePartitionedFileSetDatasetAvroSink extends BatchSink<AvroKey<GenericRecord>, NullWritable> {

  private static final String DATASET_NAME = "name";
  private static final String SCHEMA = "schema";

  @Override
  public void configure(StageConfigurer configurer) {
    configurer.setName(TimePartitionedFileSetDatasetAvroSink.class.getSimpleName());
    configurer.setDescription("An Avro sink for TimePartitionedFileSetDataset");
    configurer.addProperty(new Property(SCHEMA, "The schema of the record", true));
    configurer.addProperty(new Property(DATASET_NAME, "Name of the Time Partitioned FileSet Dataset to which the " +
      "records have to be written", true));
  }


  @Override
  public void prepareJob(BatchSinkContext context) {
    Map<String, String> sinkArgs = Maps.newHashMap();
    TimePartitionedFileSetArguments.setOutputPartitionTime(sinkArgs, context.getLogicalStartTime());
    TimePartitionedFileSet sink = context.getDataset(context.getRuntimeArguments().get(DATASET_NAME), sinkArgs);
    context.setOutput(context.getRuntimeArguments().get(DATASET_NAME), sink);
    Schema avroSchema = new Schema.Parser().parse(context.getRuntimeArguments().get(SCHEMA));
    Job job = context.getHadoopJob();
    AvroJob.setOutputKeySchema(job, avroSchema);
  }
}
