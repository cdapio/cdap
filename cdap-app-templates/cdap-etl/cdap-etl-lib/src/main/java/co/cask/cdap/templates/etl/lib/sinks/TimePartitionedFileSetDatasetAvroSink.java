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

package co.cask.cdap.templates.etl.lib.sinks;

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

import java.util.Map;

/**
 * Created by rsinha on 4/1/15.
 */
public class TimePartitionedFileSetDatasetAvroSink extends BatchSink<AvroKey<GenericRecord>, NullWritable> {
  private static final String TABLE_NAME = "name";

  /**
   * Configure the Sink.
   *
   * @param configurer {@link StageConfigurer}
   */
    @Override
    public void configure(StageConfigurer configurer) {
      configurer.setName("KVTableSink");
      configurer.setDescription("An Avro sink for TimePartitionedFileSetDataset");
      configurer.addProperty(new Property("schema", "The schema of the Structured record events", true));
      configurer.addProperty(new Property(TABLE_NAME, "Dataset Name", true));
    }

  /**
   * Prepare the Batch Job. Used to configure the Hadoop Job before starting the Batch Job.
   *
   * @param context {@link BatchSinkContext}
   */
  @Override
  public void prepareJob(BatchSinkContext context) {
    Map<String, String> sinkArgs = Maps.newHashMap();
    TimePartitionedFileSetArguments.setOutputPartitionTime(sinkArgs, context.getLogicalStartTime());
    TimePartitionedFileSet sink = context.getDataset(context.getRuntimeArguments().get(TABLE_NAME), sinkArgs);
    context.setOutput(context.getRuntimeArguments().get(TABLE_NAME), sink);
    Schema schema = new Schema.Parser().parse(context.getRuntimeArguments().get("schema"));
    Job job = context.getHadoopJob();
    AvroJob.setOutputKeySchema(context.getHadoopJob(), schema);

  }
}
