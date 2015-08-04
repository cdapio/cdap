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

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.BatchPartitionConsumer;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetArguments;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.metrics.Metrics;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A simple MapReduce that reads records from the rawRecords PartitionedFileSet and writes all records
 * that match a particular {@link Schema} to the cleanRecords PartitionedFileSet. It also keeps track of its state of
 * which partitions it has processed, so that it only processes new partitions of data each time it runs.
 */
public class DataCleansingMapReduce extends AbstractMapReduce {
  protected static final String NAME = "DataCleansingMapReduce";
  protected static final String OUTPUT_PARTITION_KEY = "output.partition.key";
  protected static final String SCHEMA_KEY = "schema.key";

  private final BatchPartitionConsumer partitionConsumer = new BatchPartitionConsumer() {
    private static final String STATE_KEY = "state.key";

    @Nullable
    @Override
    protected byte[] readBytes(DatasetContext datasetContext) {
      return ((KeyValueTable) datasetContext.getDataset(DataCleansing.CONSUMING_STATE)).read(STATE_KEY);
    }

    @Override
    protected void writeBytes(DatasetContext datasetContext, byte[] stateBytes) {
      ((KeyValueTable) datasetContext.getDataset(DataCleansing.CONSUMING_STATE)).write(STATE_KEY, stateBytes);
    }
  };

  @Override
  public void configure() {
    setName(NAME);
    setMapperResources(new Resources(1024));
    setReducerResources(new Resources(1024));
  }

  @Override
  public void beforeSubmit(MapReduceContext context) throws Exception {
    PartitionedFileSet rawRecords = partitionConsumer.getConfiguredDataset(context, DataCleansing.RAW_RECORDS);
    context.setInput(DataCleansing.RAW_RECORDS, rawRecords);

    // Each run writes its output to a partition for the league
    Long timeKey = Long.valueOf(context.getRuntimeArguments().get(OUTPUT_PARTITION_KEY));
    PartitionKey outputKey = PartitionKey.builder().addLongField("time", timeKey).build();
    Map<String, String> outputArgs = new HashMap<>();
    PartitionedFileSetArguments.setOutputPartitionKey(outputArgs, outputKey);

    PartitionedFileSet outputFileSet = context.getDataset(DataCleansing.CLEAN_RECORDS, outputArgs);
    context.setOutput(DataCleansing.CLEAN_RECORDS, outputFileSet);

    Job job = context.getHadoopJob();
    job.setMapperClass(SchemaMatchingFilter.class);

    // simply propagate the schema (if any) to be used by the mapper
    String schemaJson = context.getRuntimeArguments().get(SCHEMA_KEY);
    if (schemaJson != null) {
      job.getConfiguration().set(SCHEMA_KEY, schemaJson);
    }
  }

  @Override
  public void onFinish(boolean succeeded, MapReduceContext context) throws Exception {
    if (succeeded) {
      partitionConsumer.persist(context);
    }
  }

  /**
   * A Mapper which skips text that doesn't match a given schema.
   */
  public static class SchemaMatchingFilter extends Mapper<LongWritable, Text, NullWritable, Text> {
    public static final Schema DEFAULT_SCHEMA = Schema.recordOf("person",
                                                                Schema.Field.of("pid", Schema.of(Schema.Type.LONG)),
                                                                Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                                                                Schema.Field.of("dob", Schema.of(Schema.Type.STRING)),
                                                                Schema.Field.of("zip", Schema.of(Schema.Type.INT)));

    private SimpleSchemaMatcher schemaMatcher;
    private Metrics mapMetrics;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      // setup the schema to be used by the mapper
      String schemaJson = context.getConfiguration().get(SCHEMA_KEY);
      if (schemaJson == null) {
        schemaMatcher = new SimpleSchemaMatcher(DEFAULT_SCHEMA);
      } else {
        schemaMatcher = new SimpleSchemaMatcher(Schema.parseJson(schemaJson));
      }
    }

    @Override
    public void map(LongWritable key, Text data, Context context) throws IOException, InterruptedException {
      if (!schemaMatcher.matches(data.toString())) {
        mapMetrics.count("data.invalid", 1);
        return;
      }
      context.write(NullWritable.get(), data);
    }
  }
}
