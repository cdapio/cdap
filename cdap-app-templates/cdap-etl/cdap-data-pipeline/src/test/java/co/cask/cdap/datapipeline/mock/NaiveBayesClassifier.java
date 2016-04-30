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

package co.cask.cdap.datapipeline.mock;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import com.google.common.collect.Lists;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SparkCompute that uses a trained model to classify and tag input records.
 */
@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name(NaiveBayesClassifier.PLUGIN_NAME)
@Description("Uses a trained Naive Bayes model to classify records.")
public class NaiveBayesClassifier extends SparkCompute<StructuredRecord, StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(NaiveBayesClassifier.class);

  public static final String PLUGIN_NAME = "NaiveBayesClassifier";

  private final Config config;

  /**
   * Configuration for the NaiveBayesClassifier.
   */
  public static class Config extends PluginConfig {

    @Description("FileSet to use to load the model from.")
    private final String fileSetName;

    @Description("Path of the FileSet to load the model from.")
    private final String path;

    @Description("A space-separated sequence of words, which to classify.")
    private final String fieldToClassify;

    @Description("The field on which to set the prediction. It must be of type double.")
    private final String fieldToSet;

    public Config(String fileSetName, String path, String fieldToClassify, String fieldToSet) {
      this.fileSetName = fileSetName;
      this.path = path;
      this.fieldToClassify = fieldToClassify;
      this.fieldToSet = fieldToSet;
    }
  }

  // for unit tests, otherwise config is injected by plugin framework.
  public NaiveBayesClassifier(Config config) {
    this.config = config;
  }

  // TODO: check if the fieldToSet is already set on the input? is it double type?
  // TODO: If the field is not nullable in the input schema, create a schema that includes this field.

  @Override
  public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext context,
                                             JavaRDD<StructuredRecord> input) throws Exception {
    FileSet fileSet = context.getDataset(config.fileSetName);
    Location modelLocation = fileSet.getBaseLocation().append(config.path);
    if (!modelLocation.exists()) {
      LOG.warn("Failed to find model to use for classification. Location does not exist: {}.", modelLocation);
      return input;
    }

    // load the model from a file in the model fileset
    JavaSparkContext javaSparkContext = context.getSparkContext();
    SparkContext sparkContext = JavaSparkContext.toSparkContext(javaSparkContext);
    final NaiveBayesModel loadedModel = NaiveBayesModel.load(sparkContext, modelLocation.toURI().getPath());

    final HashingTF tf = new HashingTF(100);

    JavaRDD<StructuredRecord> output = input.map(new Function<StructuredRecord, StructuredRecord>() {
      @Override
      public StructuredRecord call(StructuredRecord structuredRecord) throws Exception {
        String text = structuredRecord.get(config.fieldToClassify);
        Vector vector = tf.transform(Lists.newArrayList(text.split(" ")));
        double prediction = loadedModel.predict(vector);

        return cloneRecord(structuredRecord)
          .set(config.fieldToSet, prediction)
          .build();
      }
    });
    return output;
  }

  // creates a builder based off the given record
  private StructuredRecord.Builder cloneRecord(StructuredRecord record) {
    Schema schema = record.getSchema();
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    for (Schema.Field field : schema.getFields()) {
      builder.set(field.getName(), record.get(field.getName()));
    }
    return builder;
  }
}
