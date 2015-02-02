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

package co.cask.cdap.conversion.app;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.data.stream.StreamBatchReadable;
import co.cask.cdap.api.dataset.lib.FileSetArguments;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSetArguments;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.stream.GenericStreamEventData;
import co.cask.cdap.conversion.avro.Converter;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
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

/**
 * MapReduce job that reads events from a stream over a given time interval and writes the events out to
 * files in avro format, as well as registering those files as partitions in Hive.
 */
public class StreamConversionMapReduce extends AbstractMapReduce {
  private static final Logger LOG = LoggerFactory.getLogger(StreamConversionMapReduce.class);
  public static final String ADAPTER_PROPERTIES = "adapter.properties";
  public static final String SCHEMA_KEY = "cdap.stream.conversion.output.schema";
  public static final String HEADERS_KEY = "cdap.stream.conversion.headers";
  private String sinkName;
  private String outputPath;
  private Long partitionTime;

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

    AdapterArguments adapterArguments = new AdapterArguments(context.getRuntimeArguments());
    sinkName = adapterArguments.getSinkName();
    partitionTime = context.getLogicalStartTime();

    // setup input for the job
    long endTime = context.getLogicalStartTime();
    long startTime = endTime - adapterArguments.getFrequency();
    StreamBatchReadable.useStreamInput(context, adapterArguments.getSourceName(), startTime, endTime,
                                       adapterArguments.getSourceFormatSpec());
    context.setMapperResources(adapterArguments.getMapperResources());

    // setup output for the job
    Map<String, String> sinkArgs = Maps.newHashMap();
    TimePartitionedFileSetArguments.setOutputPartitionTime(sinkArgs, partitionTime);
    TimePartitionedFileSet sink = context.getDataset(sinkName, sinkArgs);
    outputPath = FileSetArguments.getOutputPath(sink.getUnderlyingFileSet().getRuntimeArguments());
    context.setOutput(sinkName, sink);
    AvroJob.setOutputKeySchema(job, adapterArguments.getSinkSchema());

    // set configuration the mappers will need
    job.getConfiguration().set(SCHEMA_KEY, adapterArguments.getSinkSchema().toString());
    if (adapterArguments.getHeadersStr() != null) {
      job.getConfiguration().set(HEADERS_KEY, adapterArguments.getHeadersStr());
    }
    job.setJobName("adapter.stream-conversion." + adapterArguments.getSourceName()
                     + ".to." + sinkName + "." + partitionTime);
  }

  @Override
  public void onFinish(boolean succeeded, MapReduceContext context) throws Exception {
    if (succeeded) {
      TimePartitionedFileSet converted = context.getDataset(sinkName);

      LOG.info("Adding partition for time {} with path {} to dataset '{}'", partitionTime, outputPath, sinkName);
      converted.addPartition(partitionTime, outputPath);
    }
  }

  /**
   * Mapper that reads events from a stream and writes them out as Avro.
   */
  public static class StreamConversionMapper extends
    Mapper<LongWritable, GenericStreamEventData<Object>, AvroKey<GenericRecord>, NullWritable> {
    private Schema schema;
    private String[] headers;
    private Converter converter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      schema = new Schema.Parser().parse(context.getConfiguration().get(SCHEMA_KEY));
      String headersStr = context.getConfiguration().get(HEADERS_KEY);
      headers = headersStr == null ? new String[0] : headersStr.split(",");
      converter = new Converter(schema, headers);
    }

    @Override
    public void map(LongWritable timestamp, GenericStreamEventData<Object> streamEvent, Context context)
      throws IOException, InterruptedException {
      Map<String, String> headers = Objects.firstNonNull(streamEvent.getHeaders(), ImmutableMap.<String, String>of());
      GenericRecord record = converter.convert(streamEvent.getBody(), timestamp.get(), headers);
      context.write(new AvroKey<GenericRecord>(record), NullWritable.get());
    }
  }
}
