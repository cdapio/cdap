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
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.etl.api.batch.SparkPluginContext;
import co.cask.cdap.etl.api.batch.SparkSink;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * Spark Sink plugin that trains a model based upon whether messages are spam or not, and then classifies messages.
 * Also persists the trained model to a file in a FileSet.
 */
@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name(NaiveBayesTrainer.PLUGIN_NAME)
@Description("Trains a model based upon whether messages are spam or not.")
public final class NaiveBayesTrainer extends SparkSink<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(NaiveBayesTrainer.class);

  public static final String PLUGIN_NAME = "NaiveBayesTrainer";
  public static final String TEXTS_TO_CLASSIFY = "textsToClassify";
  public static final String CLASSIFIED_TEXTS = "classifiedTexts";

  private final Config config;

  /**
   * Configuration for the NaiveBayesTrainer.
   */
  public static class Config extends PluginConfig {

    @Description("FileSet to use to load the model from.")
    private final String fileSetName;

    @Description("Path of the FileSet to load the model from.")
    private final String path;

    @Description("A space-separated sequence of words, which to use for classification.")
    private final String fieldToClassify;

    @Description("The field from which to get the prediction. It must be of type double.")
    private final String predictionField;

    public Config(String fileSetName, String path, String fieldToClassify, String predictionField) {
      this.fileSetName = fileSetName;
      this.path = path;
      this.fieldToClassify = fieldToClassify;
      this.predictionField = predictionField;
    }
  }

  public NaiveBayesTrainer(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    pipelineConfigurer.addStream(TEXTS_TO_CLASSIFY);
    pipelineConfigurer.createDataset(CLASSIFIED_TEXTS, KeyValueTable.class);
    pipelineConfigurer.createDataset(config.fileSetName, FileSet.class, FileSetProperties.builder()
      .setInputFormat(TextInputFormat.class)
      .setOutputFormat(TextOutputFormat.class)
      .setOutputProperty(TextOutputFormat.SEPERATOR, ":").build());
  }

  @Override
  public void prepareRun(SparkPluginContext context) throws Exception {
    // no-op; no need to do anything
  }

  @Override
  public void run(SparkExecutionPluginContext sparkContext, JavaRDD<StructuredRecord> input) throws Exception {
    Preconditions.checkArgument(input.count() != 0, "Input RDD is empty.");

    final HashingTF tf = new HashingTF(100);
    JavaRDD<LabeledPoint> trainingData = input.map(new Function<StructuredRecord, LabeledPoint>() {
      @Override
      public LabeledPoint call(StructuredRecord record) throws Exception {
        String text = record.get(config.fieldToClassify);
        return new LabeledPoint((Double) record.get(config.predictionField),
                                tf.transform(Lists.newArrayList(text.split(" "))));
      }
    });

    trainingData.cache();

    final NaiveBayesModel model = NaiveBayes.train(trainingData.rdd(), 1.0);

    // save the model to a file in the output FileSet
    JavaSparkContext javaSparkContext = sparkContext.getSparkContext();
    FileSet outputFS = sparkContext.getDataset(config.fileSetName);
    model.save(JavaSparkContext.toSparkContext(javaSparkContext),
               outputFS.getBaseLocation().append(config.path).toURI().getPath());

    JavaPairRDD<Long, String> textsToClassify = sparkContext.fromStream(TEXTS_TO_CLASSIFY, String.class);
    JavaRDD<Vector> featuresToClassify = textsToClassify.map(new Function<Tuple2<Long, String>, Vector>() {
      @Override
      public Vector call(Tuple2<Long, String> longWritableTextTuple2) throws Exception {
        String text = longWritableTextTuple2._2();
        return tf.transform(Lists.newArrayList(text.split(" ")));
      }
    });

    JavaRDD<Double> predict = model.predict(featuresToClassify);
    LOG.info("Predictions: {}", predict.collect());

    // key the predictions with the message
    JavaPairRDD<String, Double> keyedPredictions = textsToClassify.values().zip(predict);

    // convert to byte[],byte[] to write to data
    JavaPairRDD<byte[], byte[]> bytesRDD =
      keyedPredictions.mapToPair(new PairFunction<Tuple2<String, Double>, byte[], byte[]>() {
        @Override
        public Tuple2<byte[], byte[]> call(Tuple2<String, Double> tuple) throws Exception {
          return new Tuple2<>(Bytes.toBytes(tuple._1()), Bytes.toBytes(tuple._2()));
        }
      });

    sparkContext.saveAsDataset(bytesRDD, CLASSIFIED_TEXTS);
  }
}
