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

import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetArguments;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetProperties;
import co.cask.cdap.templates.etl.api.PipelineConfigurer;
import co.cask.cdap.templates.etl.api.Property;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.batch.BatchSink;
import co.cask.cdap.templates.etl.api.batch.BatchSinkContext;
import co.cask.cdap.templates.etl.api.config.ETLStage;
import co.cask.cdap.templates.etl.common.ETLUtils;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * CDAP {@link PartitionedFileSet} Batch Sink
 */
public class PartitionedFileSetSink extends BatchSink<LongWritable, Text> {

  private static final String FILESET_NAME = "filesetName";
  private static final String PARTITION_KEY = "partitionKey";
  private static final String PARTITIONING = "partitioning";

  @Override
  public void configure(StageConfigurer configurer) {
    configurer.setName(PartitionedFileSetSink.class.getSimpleName());
    configurer.setDescription("CDAP Partitioned fileSet batch sink");
    configurer.addProperty(new Property(FILESET_NAME, "Partitioned fileset name", true));
    configurer.addProperty(new Property(PARTITION_KEY, "Partition key in JSON format to use while writing " +
      "ex: {\"league\" : \"nfl\", \"season\" : \"1980\"}", true));
    configurer.addProperty(new Property(PARTITIONING, "Partitioning to use for the Partitioned FileSet in JSON " +
      "format. Should be provided if you want the pipeline to create the fileset. ex: " +
      "{\"league\" : \"string\", \"season\" : \"int\"}", false));
    configurer.addProperty(new Property(TextOutputFormat.SEPERATOR, "The output format separator. Must be provided " +
      "if you want the pipeline to create the fileset", false));
  }

  @Override
  public void configurePipeline(ETLStage stageConfig, PipelineConfigurer pipelineConfigurer) {
    // if the partitioning is provided then we should try to create the fileset here
    if (!Strings.isNullOrEmpty(stageConfig.getProperties().get(PARTITIONING))) {
      String pfsName = stageConfig.getProperties().get(FILESET_NAME);
      String separator = stageConfig.getProperties().get(TextOutputFormat.SEPERATOR);
      Preconditions.checkArgument(!Strings.isNullOrEmpty(pfsName), "PartitionedFileSet name must be given.");
      Preconditions.checkArgument(!Strings.isNullOrEmpty(pfsName), "A separator for " +
        TextOutputFormat.SEPERATOR + "must be given");
      pipelineConfigurer.createDataset(pfsName, PartitionedFileSet.class.getName(),
                                       PartitionedFileSetProperties.builder()
        .setPartitioning(ETLUtils.createPartitioning(stageConfig.getProperties().get(PARTITIONING)))
        .setInputFormat(TextInputFormat.class)
        .setOutputFormat(TextOutputFormat.class)
        .setOutputProperty(TextOutputFormat.SEPERATOR, separator)
        .build());
    }
  }

  @Override
  public void prepareJob(BatchSinkContext context) {
    Map<String, String> inputArgs = Maps.newHashMap();
    PartitionedFileSetArguments.setOutputPartitionKey(inputArgs,
                                                      buildParitionKey(
                                                        context.getRuntimeArguments().get(PARTITION_KEY)));
    PartitionedFileSet input = context.getDataset(context.getRuntimeArguments().get(FILESET_NAME), inputArgs);
    context.setOutput(context.getRuntimeArguments().get(FILESET_NAME), input);
  }

  /**
   * Builds a {@link PartitionKey} from user supplied JSON string.
   *
   * @param paritionKeyString the partition key JSON string
   * @return {@link PartitionKey}
   */
  private PartitionKey buildParitionKey(String paritionKeyString) {
    PartitionKey.Builder builder = PartitionKey.builder();
    Type stringStringMap = new TypeToken<Map<String, String>>() {
    }.getType();
    Map<String, String> paritionKeyMap = new Gson().fromJson(paritionKeyString, stringStringMap);
    for (Map.Entry<String, String> entrySet : paritionKeyMap.entrySet()) {
      builder.addField(entrySet.getKey(), entrySet.getValue());
    }
    return builder.build();
  }
}
