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

package $package;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.etl.api.batch.SparkPluginContext;
import co.cask.cdap.etl.api.batch.SparkSink;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * SparkSink plugin that counts how many times each word appears in records input to it and stores the result in
 * a KeyValueTable.
 */
@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name(WordCountSink.NAME)
@Description("Counts how many times each word appears in all records input to the aggregator.")
public class WordCountSink extends SparkSink<StructuredRecord> {
  public static final String NAME = "WordCount";
  private final Conf config;

  /**
   * Config properties for the plugin.
   */
  public static class Conf extends PluginConfig {
    @Description("The field from the input records containing the words to count.")
    private String field;

    @Description("The name of the KeyValueTable to write to.")
    private String tableName;
  }

  public WordCountSink(Conf config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    // any static configuration validation should happen here.
    // We will check that the field is in the input schema and is of type string.
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    if (inputSchema != null) {
      WordCount wordCount = new WordCount(config.field);
      wordCount.validateSchema(inputSchema);
    }
    pipelineConfigurer.createDataset(config.tableName, KeyValueTable.class, DatasetProperties.EMPTY);
  }

  @Override
  public void prepareRun(SparkPluginContext sparkPluginContext) throws Exception {
    // no-op
  }

  @Override
  public void run(SparkExecutionPluginContext sparkExecutionPluginContext,
                  JavaRDD<StructuredRecord> javaRDD) throws Exception {
    WordCount wordCount = new WordCount(config.field);
    JavaPairRDD outputRDD = wordCount.countWords(javaRDD)
      .mapToPair(new PairFunction<Tuple2<String, Long>, byte[], byte[]>() {
        @Override
        public Tuple2<byte[], byte[]> call(Tuple2<String, Long> stringLongTuple2) throws Exception {
          return new Tuple2<>(Bytes.toBytes(stringLongTuple2._1()), Bytes.toBytes(stringLongTuple2._2()));
        }
      });
    sparkExecutionPluginContext.saveAsDataset(outputRDD, config.tableName);
  }
}
