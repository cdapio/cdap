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
  private static final Schema SCHEMA = new Schema.Parser().parse(StreamConversionApp.SCHEMA_STRING);

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
    AvroJob.setOutputKeySchema(job, SCHEMA);

    // read 5 minutes of events from the stream, ending at the logical start time of this run
    long logicalTime = context.getLogicalStartTime();
    StreamBatchReadable.useStreamInput(context, "events", logicalTime - TimeUnit.MINUTES.toMillis(5), logicalTime);

    // each run writes its output to a partition with the logical start time.
    TimePartitionedFileSetArguments.setOutputPartitionTime(dsArguments, logicalTime);
    TimePartitionedFileSet partitionedFileSet = context.getDataset("converted", dsArguments);
    context.setOutput("converted", partitionedFileSet);

    LOG.info("Output location for new partition is: {}",
             partitionedFileSet.getEmbeddedFileSet().getOutputLocation().toURI().toString());
  }

  @Override
  public void onFinish(boolean succeeded, MapReduceContext context) throws Exception {
    if (succeeded) {
      // TODO this should be done by the output committer (CDAP-1227)
      TimePartitionedFileSet converted = context.getDataset("converted", dsArguments);
      String outputPath = FileSetArguments.getOutputPath(converted.getEmbeddedFileSet().getRuntimeArguments());
      Long partitionTime = context.getLogicalStartTime();

      LOG.info("Adding partition for time {} with path {} to dataset '{}'", partitionTime, outputPath, "converted");
      converted.addPartition(partitionTime, outputPath);
    }
  }

  /**
   * Mapper that reads events from a stream and writes them out as Avro.
   */
  public static class StreamConversionMapper extends
    Mapper<LongWritable, StreamEvent, AvroKey<GenericRecord>, NullWritable> {

    @Override
    public void map(LongWritable timestamp, StreamEvent streamEvent, Context context)
      throws IOException, InterruptedException {
      GenericRecordBuilder recordBuilder = new GenericRecordBuilder(SCHEMA)
        .set("time", streamEvent.getTimestamp())
        .set("body", Bytes.toString(streamEvent.getBody()));
      GenericRecord record = recordBuilder.build();
      context.write(new AvroKey<GenericRecord>(record), NullWritable.get());
    }
  }
}
