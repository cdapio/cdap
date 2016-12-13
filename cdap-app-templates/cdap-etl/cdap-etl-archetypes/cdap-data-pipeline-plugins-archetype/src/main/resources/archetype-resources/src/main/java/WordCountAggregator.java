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
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchAggregator;

import java.util.Iterator;
import java.util.regex.Pattern;

/**
 * Aggregator that counts how many times each word appears in records input to the aggregator.
 */
@Plugin(type = BatchAggregator.PLUGIN_TYPE)
@Name(WordCountAggregator.NAME)
@Description("Counts how many times each word appears in all records input to the aggregator.")
public class WordCountAggregator extends BatchAggregator<String, StructuredRecord, StructuredRecord> {
  public static final String NAME = "WordCount";
  public static final Schema OUTPUT_SCHEMA = Schema.recordOf(
    "wordCount",
    Schema.Field.of("word", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("count", Schema.of(Schema.Type.LONG))
  );
  private static final Pattern WHITESPACE = Pattern.compile("\\s");
  private final Conf config;

  /**
   * Config properties for the plugin.
   */
  public static class Conf extends PluginConfig {
    @Description("The field from the input records containing the words to count.")
    private String field;
  }

  public WordCountAggregator(Conf config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    // any static configuration validation should happen here.
    // We will check that the field is in the input schema and is of type string.
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    // a null input schema means its unknown until runtime, or its not constant
    if (inputSchema != null) {
      // if the input schema is constant and known at configure time, check that the input field exists and is a string.
      Schema.Field inputField = inputSchema.getField(config.field);
      if (inputField == null) {
        throw new IllegalArgumentException(
          String.format("Field '%s' does not exist in input schema %s.", config.field, inputSchema));
      }
      Schema fieldSchema = inputField.getSchema();
      Schema.Type fieldType = fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
      if (fieldType != Schema.Type.STRING) {
        throw new IllegalArgumentException(
          String.format("Field '%s' is of illegal type %s. Must be of type %s.",
                        config.field, fieldType, Schema.Type.STRING));
      }
    }
    // set the output schema so downstream stages will know their input schema.
    pipelineConfigurer.getStageConfigurer().setOutputSchema(OUTPUT_SCHEMA);
  }

  @Override
  public void groupBy(StructuredRecord input, Emitter<String> groupKeyEmitter) throws Exception {
    String val = input.get(config.field);
    if (val == null) {
      return;
    }

    for (String word : WHITESPACE.split(val)) {
      groupKeyEmitter.emit(word);
    }
  }

  @Override
  public void aggregate(String groupKey, Iterator<StructuredRecord> groupValues,
                        Emitter<StructuredRecord> emitter) throws Exception {
    long count = 0;
    while (groupValues.hasNext()) {
      groupValues.next();
      count++;
    }
    emitter.emit(StructuredRecord.builder(OUTPUT_SCHEMA).set("word", groupKey).set("count", count).build());
  }
}
