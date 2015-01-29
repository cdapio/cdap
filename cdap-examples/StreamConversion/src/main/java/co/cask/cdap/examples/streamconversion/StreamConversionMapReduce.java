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

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.stream.StreamBatchReadable;
import co.cask.cdap.api.dataset.lib.FileSetArguments;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSetArguments;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
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

  private final Map<String, String> dsArguments = Maps.newHashMap();

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

    // we will read <interval.minutes> of events from the stream, ending at <logical.time>
    // (default: 5 minutes up to logical start time of the job).
    long logicalTime = context.getLogicalStartTime();
    long intervalLength = TimeUnit.MINUTES.toMillis(5);;

    // configure the stream input format
    StreamBatchReadable.useStreamInput(context, "events", logicalTime - intervalLength, logicalTime);

    // configure the output dataset
    TimePartitionedFileSetArguments.setOutputPartitionTime(dsArguments, logicalTime);
    TimePartitionedFileSet partitionedFileSet = context.getDataset("converted", dsArguments);

    // this schema is our schema, which is slightly different than avro's schema in terms of the types it supports.
    // Incompatibilities will surface here and cause the job to fail.
    String schemaStr = partitionedFileSet.getOutputFormatConfiguration().get("schema");
    job.getConfiguration().set(SCHEMA_KEY, schemaStr);
    Schema schema = new Schema.Parser().parse(schemaStr);
    AvroJob.setOutputKeySchema(job, schema);

    // each job will output to a partition with its logical start time.
    LOG.info("Output location for new partition is: {}",
             partitionedFileSet.getUnderlyingFileSet().getOutputLocation().toURI().toString());
    context.setOutput("converted", partitionedFileSet);
  }

  @Override
  public void onFinish(boolean succeeded, MapReduceContext context) throws Exception {
    if (succeeded) {
      // TODO this should be done by the output committer (CDAP-1227)
      TimePartitionedFileSet converted = context.getDataset("converted", dsArguments);

      String outputPath = FileSetArguments.getOutputPath(converted.getUnderlyingFileSet().getRuntimeArguments());
      Long time = TimePartitionedFileSetArguments.getOutputPartitionTime(converted.getRuntimeArguments());
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
      GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema)
        .set("ts", streamEvent.getTimestamp())
        .set("body", Bytes.toString(streamEvent.getBody()));
      GenericRecord record = recordBuilder.build();
      context.write(new AvroKey<GenericRecord>(record), NullWritable.get());
    }
  }
}
