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

package org.example.plugin;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * SparkCompute plugin that counts how many times each word appears in records input to the compute stage.
 */
@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name(WordCountCompute.NAME)
@Description("Counts how many times each word appears in all records input to the aggregator.")
public class WordCountCompute extends SparkCompute<StructuredRecord, StructuredRecord> {
  public static final String NAME = "WordCount";
  public static final Schema OUTPUT_SCHEMA = Schema.recordOf(
    "wordCount",
    Schema.Field.of("word", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("count", Schema.of(Schema.Type.LONG))
  );
  private final Conf config;

  /**
   * Config properties for the plugin.
   */
  public static class Conf extends PluginConfig {
    @Description("The field from the input records containing the words to count.")
    private String field;
  }

  public WordCountCompute(Conf config) {
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
    // set the output schema so downstream stages will know their input schema.
    pipelineConfigurer.getStageConfigurer().setOutputSchema(OUTPUT_SCHEMA);
  }

  @Override
  public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext sparkExecutionPluginContext,
                                             JavaRDD<StructuredRecord> javaRDD) throws Exception {
    WordCount wordCount = new WordCount(config.field);
    return wordCount.countWords(javaRDD)
      .flatMap(new FlatMapFunction<Tuple2<String, Long>, StructuredRecord>() {
        @Override
        public Iterable<StructuredRecord> call(Tuple2<String, Long> stringLongTuple2) throws Exception {
          List<StructuredRecord> output = new ArrayList<>();
          output.add(StructuredRecord.builder(OUTPUT_SCHEMA)
                       .set("word", stringLongTuple2._1())
                       .set("count", stringLongTuple2._2())
                       .build());
          return output;
        }
      });
  }
}
