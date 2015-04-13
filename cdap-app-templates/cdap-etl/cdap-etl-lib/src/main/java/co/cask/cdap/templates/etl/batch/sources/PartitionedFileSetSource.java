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

package co.cask.cdap.templates.etl.batch.sources;

import co.cask.cdap.api.dataset.lib.PartitionFilter;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetArguments;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetProperties;
import co.cask.cdap.templates.etl.api.PipelineConfigurer;
import co.cask.cdap.templates.etl.api.Property;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.batch.BatchSource;
import co.cask.cdap.templates.etl.api.batch.BatchSourceContext;
import co.cask.cdap.templates.etl.api.config.ETLStage;
import co.cask.cdap.templates.etl.common.ETLUtils;
import com.google.common.base.Joiner;
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
 * CDAP {@link PartitionedFileSet} Batch Source
 */
public class PartitionedFileSetSource extends BatchSource<LongWritable, Text> {

  private static final String FILESET_NAME = "filesetName";
  private static final String FILTER = "filter";
  private static final String PARTITIONING = "partitioning";
  private static final String VALUE = "value";
  private static final String LOWER = "lower";
  private static final String UPPER = "upper";


  @Override
  public void configure(StageConfigurer configurer) {
    configurer.setName(PartitionedFileSetSource.class.getSimpleName());
    configurer.setDescription("CDAP Partitioned fileSet batch source");
    configurer.addProperty(new Property(FILESET_NAME, "Partitioned fileset name", true));
    configurer.addProperty(new Property(FILTER, "Partition filter to use if any while reading in JSON format " +
      "ex: {\"league.value\": \"nfl\",\"season.lower\": \"1980\",\"season.upper\": \"1990\"}", false));
    configurer.addProperty(new Property(PARTITIONING, "Partitioning to use for the Partitioned FileSet. " +
      "Must be provided if you want the pipeline to create the fileset. ex: " +
      "{\"league\" : \"string\", \"season\" : \"int\"}", false));
    configurer.addProperty(new Property(TextOutputFormat.SEPERATOR, "The output format separator must be provided " +
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
  public void prepareJob(BatchSourceContext context) {
    Map<String, String> inputArgs = Maps.newHashMap();
    PartitionedFileSetArguments.setInputPartitionFilter(inputArgs,
                                                        createPartitionFilter(
                                                          context.getRuntimeArguments().get(FILTER)));
    PartitionedFileSet input = context.getDataset(context.getRuntimeArguments().get(FILESET_NAME), inputArgs);
    context.setInput(context.getRuntimeArguments().get(FILESET_NAME), input);
  }

  /**
   * creates a {@link PartitionFilter} from a given string in JSON format
   *
   * @param filterString partition filter JSON string
   * @return {@link PartitionFilter}
   */
  private PartitionFilter createPartitionFilter(String filterString) {
    PartitionFilter.Builder builder = PartitionFilter.builder();
    Type stringStringMap = new TypeToken<Map<String, String>>() {
    }.getType();
    Map<String, String> filters = new Gson().fromJson(filterString, stringStringMap);
    for (Map.Entry<String, String> entrySet : filters.entrySet()) {
      String key = entrySet.getKey();
      if (key.endsWith(VALUE)) {
        builder.addValueCondition(key.split("\\.")[0], entrySet.getValue());
      } else if (key.endsWith(LOWER)) {
        String fieldName = key.split("\\.")[0];
        // if the upper range is found for  this lowe range add it to PartitionFilter else just ignore
        String upperRange = Joiner.on(".").join(fieldName, UPPER);
        if (filters.containsKey(upperRange)) {
          builder.addRangeCondition(fieldName, entrySet.getValue(), filters.get(upperRange));
        }
      }
    }
    return builder.build();
  }
}
