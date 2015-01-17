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

package co.cask.cdap.conversion;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.stream.StreamBatchReadable;
import co.cask.cdap.api.dataset.lib.FileSetArguments;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSetArguments;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import com.google.common.base.Preconditions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * MapReduce job that reads events from a stream over a given time interval and writes the events out to a FileSet
 * in avro format.
 */
public class StreamConversionMapReduce extends AbstractMapReduce {
  private static final Logger LOG = LoggerFactory.getLogger(StreamConversionMapReduce.class);

  public static final String SCHEMA_KEY = "cdap.stream.conversion.output.schema";
  public static final String MAPPER_MEMORY = "cdap.stream.conversion.mapper.memorymb";

  // impala only supports scalar types, so we can't include headers as a map
  // instead, allow a comma separated list of headers to be set in runtime args as columns to include.
  // assumes the schema of the output dataset is appropriately named.

  @Override
  public void configure() {
    setDescription("Job to read a chunk of stream events and write them to a FileSet");
    setMapperResources(new Resources(512));
  }

  @Override
  public void beforeSubmit(MapReduceContext context) throws Exception {
    Job job = context.getHadoopJob();
    job.setMapperClass(StreamConversionMapper.class);
    job.setNumReduceTasks(0);
    job.setMapOutputKeyClass(AvroKey.class);
    job.setMapOutputValueClass(NullWritable.class);

    Map<String, String> runtimeArgs = context.getRuntimeArguments();
    // set memory if present in runtime args

    // TODO: move this capability into the framework.
    String mapperMemoryMBStr = runtimeArgs.get(MAPPER_MEMORY);
    if (mapperMemoryMBStr != null) {
      int mapperMemoryMB = Integer.parseInt(mapperMemoryMBStr);
      job.getConfiguration().setInt(Job.MAP_MEMORY_MB, mapperMemoryMB);
      // Also set the Xmx to be smaller than the container memory.
      job.getConfiguration().set(Job.MAP_JAVA_OPTS, "-Xmx" + (int) (mapperMemoryMB * 0.8) + "m");
    }

    // the time when this MapReduce job is supposed to start if this job is started by the scheduler
    long logicalStartTime = context.getLogicalStartTime();
    long runFrequency = TimeUnit.MINUTES.toMillis(5);
    StreamBatchReadable.useStreamInput(context, "events", logicalStartTime - runFrequency, logicalStartTime);

    // this schema is our schema, which is slightly different than avro's schema in terms of the types it supports.
    // Incompatibilities will surface here and cause the job to fail.
    TimePartitionedFileSet partitionedFileSet = context.getDataset("converted");
    String schemaStr = partitionedFileSet.getOutputFormatConfiguration().get("schema");
    job.getConfiguration().set(SCHEMA_KEY, schemaStr);
    Schema schema = new Schema.Parser().parse(schemaStr);
    AvroJob.setOutputKeySchema(job, schema);

    // each job will output to a partition with its logical start time.
    LOG.info("Output location for new partition is: {}",
             partitionedFileSet.getUnderlyingFileSet().getOutputLocation().toURI().toString());
    context.setOutput("converted");
  }

  @Override
  public void onFinish(boolean succeeded, MapReduceContext context) throws Exception {
    if (succeeded) {
      TimePartitionedFileSet converted = context.getDataset("converted");

      String outputPath = FileSetArguments.getOutputPath(converted.getUnderlyingFileSet().getRuntimeArguments());
      Long time = TimePartitionedFileSetArguments.getOutputPartitionTime(
        FileSetProperties.getOutputProperties(converted.getRuntimeArguments()));
      Preconditions.checkNotNull(time, "Output partition time is null.");

      LOG.info("Adding partition for time {} with path {} to dataset '{}'", time, outputPath, "converted");
      converted.addPartition(time, outputPath);
    }
  }

  /**
   * Mapper that reads events from a stream and writes them out as Avro.
   */
  public static class StreamConversionMapper extends
    Mapper<LongWritable, StreamEvent, AvroKey<GenericRecord>, NullWritable> {
    private Schema schema;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      schema = new Schema.Parser().parse(context.getConfiguration().get(SCHEMA_KEY));
    }

    @Override
    public void map(LongWritable timestamp, StreamEvent streamEvent, Context context)
      throws IOException, InterruptedException {
      // TODO: replace with stream event -> avro record conversion
      GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema)
        .set("ts", streamEvent.getTimestamp())
        .set("body", Bytes.toString(streamEvent.getBody()));
      GenericRecord record = recordBuilder.build();
      context.write(new AvroKey<GenericRecord>(record), NullWritable.get());
    }
  }
}
