/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.batch;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.DynamicPartitioner;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetArguments;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetProperties;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.mapreduce.MapReduceTaskContext;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * App used to test whether M/R can use DynamicPartitioner with AvroKeyOutputFormat.
 */
public class AppWithMapReduceUsingAvroDynamicPartitioner extends AbstractApplication {

  public static final String INPUT_DATASET = "INPUT_DATASET_NAME";
  public static final String OUTPUT_DATASET = "OUTPUT_DATASET_NAME";

  static final String OUTPUT_PARTITION_KEY = "output.partition.key";

  static final String SCHEMA_STRING = Schema.recordOf(
    "record",
    Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("zip", Schema.of(Schema.Type.INT))).toString();

  static final org.apache.avro.Schema SCHEMA = new org.apache.avro.Schema.Parser().parse(SCHEMA_STRING);


  @Override
  public void configure() {
    setName("AppWithMapReduceUsingAvroDynamicPartitioner");
    setDescription("Application with MapReduce job using file as dataset");
    createDataset(INPUT_DATASET, KeyValueTable.class);

    createDataset(OUTPUT_DATASET, PartitionedFileSet.class, PartitionedFileSetProperties.builder()
      // Properties for partitioning
      .setPartitioning(Partitioning.builder().addLongField("time").addIntField("zip").build())
        // Properties for file set
      .setInputFormat(AvroKeyInputFormat.class)
      .setOutputFormat(AvroKeyOutputFormat.class)
        // Properties for Explore (to create a partitioned Hive table)
      .setEnableExploreOnCreate(true)
      .setSerDe("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
      .setExploreInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
      .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
      .setTableProperty("avro.schema.literal", SCHEMA_STRING)
      .build());

    addMapReduce(new DynamicPartitioningMapReduce());
  }

  /**
   * Partitions the records based upon a runtime argument (time) and a field extracted from the text being written (zip)
   */
  public static final class TimeAndZipPartitioner extends DynamicPartitioner<AvroKey<GenericRecord>, NullWritable> {

    private Long outputPartitionKey;

    @Override
    public void initialize(MapReduceTaskContext<AvroKey<GenericRecord>, NullWritable> mapReduceTaskContext) {
      this.outputPartitionKey = Long.valueOf(mapReduceTaskContext.getRuntimeArguments().get(OUTPUT_PARTITION_KEY));
    }

    @Override
    public PartitionKey getPartitionKey(AvroKey<GenericRecord> record, NullWritable value) {
      return PartitionKey.builder()
        .addLongField("time", outputPartitionKey)
        .addIntField("zip", (int) record.datum().get("zip"))
        .build();
    }
  }

  /**
   * MapReduce job that dynamically partitions records based upon the 'zip' field in the record.
   */
  public static final class DynamicPartitioningMapReduce extends AbstractMapReduce {
    public static final Map<String, String> METADATA = ImmutableMap.of("someKey1", "thisValue",
                                                                       "someKey2", "otherValue",
                                                                       "finalKey", "final.Value",
                                                                       "post.-final", "actually.final.value");

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      context.setInput(INPUT_DATASET);

      Map<String, String> outputDatasetArgs = new HashMap<>();
      PartitionedFileSetArguments.setDynamicPartitioner(outputDatasetArgs, TimeAndZipPartitioner.class);
      PartitionedFileSetArguments.setOutputPartitionMetadata(outputDatasetArgs, METADATA);
      context.addOutput(OUTPUT_DATASET, outputDatasetArgs);

      Job job = context.getHadoopJob();
      job.setMapperClass(FileMapper.class);
      job.setNumReduceTasks(0);

      AvroJob.setOutputKeySchema(job, SCHEMA);
    }
  }

  public static class FileMapper extends Mapper<byte[], byte[], AvroKey<GenericRecord>, NullWritable> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);
    }

    @Override
    public void map(byte[] key, byte[] data, Context context) throws IOException, InterruptedException {
      JsonObject jsonObject = new JsonParser().parse(Bytes.toString(data)).getAsJsonObject();
      GenericRecordBuilder recordBuilder = new GenericRecordBuilder(SCHEMA)
        .set("name", jsonObject.get("name").getAsString())
        .set("zip", jsonObject.get("zip").getAsInt());
      GenericRecord record = recordBuilder.build();
      context.write(new AvroKey<>(record), NullWritable.get());
    }
  }

}
