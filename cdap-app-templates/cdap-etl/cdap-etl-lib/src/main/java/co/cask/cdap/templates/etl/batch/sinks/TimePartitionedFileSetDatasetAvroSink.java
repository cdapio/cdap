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

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSetArguments;
import co.cask.cdap.templates.etl.api.Property;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.batch.BatchSink;
import co.cask.cdap.templates.etl.api.batch.BatchSinkContext;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A {@link BatchSink} to write to {@link TimePartitionedFileSet}
 */
public class TimePartitionedFileSetDatasetAvroSink extends BatchSink<AvroKey<GenericRecord>, NullWritable> {

  static final String DATASETNAME = "name";

  @Override
  public void configure(StageConfigurer configurer) {
    configurer.setName(TimePartitionedFileSetDatasetAvroSink.class.getName());
    configurer.setDescription("An Avro sink for TimePartitionedFileSetDataset");
    configurer.addProperty(new Property("schema", "The schema of the record", true));
    configurer.addProperty(new Property(DATASETNAME, "Name of the Time Partitioned FileSet Dataset to which the " +
      "records have to be written", true));
  }


  @Override
  public void prepareJob(BatchSinkContext context) {
    Map<String, String> sinkArgs = Maps.newHashMap();
    TimePartitionedFileSetArguments.setOutputPartitionTime(sinkArgs, context.getLogicalStartTime());
    TimePartitionedFileSet sink = context.getDataset(context.getRuntimeArguments().get(DATASETNAME), sinkArgs);
    context.setOutput(context.getRuntimeArguments().get(DATASETNAME), sink);
    Schema streamBodySchema = null;
    try {
      streamBodySchema = Schema.parseJson(context.getRuntimeArguments().get("schema"));
    } catch (IOException e) {
      Throwables.propagate(e);
    }
    List<Schema.Field> fields = Lists.newArrayList(
      Schema.Field.of("ts", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("headers", Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING))));
    fields.addAll(streamBodySchema.getFields());
    Schema streamSchema = Schema.recordOf("streamEvent", fields);
    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(streamSchema.toString());
    Job job = context.getHadoopJob();
    AvroJob.setOutputKeySchema(job, avroSchema);
  }
}
