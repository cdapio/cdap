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

package co.cask.cdap.etl.datapipeline.mock;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.SparkPluginContext;
import co.cask.cdap.etl.api.batch.SparkSink;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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
@Name("SpamOrHam")
@Description("Trains a model based upon whether messages are spam or not.")
public final class SpamOrHam extends SparkSink<StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(SpamOrHam.class);

  private static final String OUTPUT_FILESET = "modelFileSet";

  public static final String TEXTS_TO_CLASSIFY = "textsToClassify";
  public static final String CLASSIFIED_TEXTS = "classifiedTexts";

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    pipelineConfigurer.addStream(TEXTS_TO_CLASSIFY);
    pipelineConfigurer.createDataset(CLASSIFIED_TEXTS, KeyValueTable.class);
    pipelineConfigurer.createDataset(OUTPUT_FILESET, FileSet.class, FileSetProperties.builder()
      .setInputFormat(TextInputFormat.class)
      .setOutputFormat(TextOutputFormat.class)
      .setOutputProperty(TextOutputFormat.SEPERATOR, ":").build());
  }

  @Override
  public void prepareRun(SparkPluginContext context) throws Exception {
    // no-op; no need to do anything
  }

  @Override
  public void run(SparkPluginContext sparkContext, JavaRDD<StructuredRecord> input) throws Exception {
    Preconditions.checkArgument(input.count() != 0, "Input RDD is empty.");
    final JavaRDD<StructuredRecord> spam = input.filter(new Function<StructuredRecord, Boolean>() {
      @Override
      public Boolean call(StructuredRecord record) throws Exception {
        return record.get(SpamMessage.IS_SPAM_FIELD);
      }
    });

    JavaRDD<StructuredRecord> ham = input.filter(new Function<StructuredRecord, Boolean>() {
      @Override
      public Boolean call(StructuredRecord record) throws Exception {
        return !(Boolean) record.get(SpamMessage.IS_SPAM_FIELD);
      }
    });

    final HashingTF tf = new HashingTF(100);
    JavaRDD<Vector> spamFeatures = spam.map(new Function<StructuredRecord, Vector>() {
      @Override
      public Vector call(StructuredRecord structuredRecord) throws Exception {
        String text = structuredRecord.get(SpamMessage.TEXT_FIELD);
        return tf.transform(Lists.newArrayList(text.split(" ")));
      }
    });

    JavaRDD<Vector> hamFeatures = ham.map(new Function<StructuredRecord, Vector>() {
      @Override
      public Vector call(StructuredRecord structuredRecord) throws Exception {
        String text = structuredRecord.get(SpamMessage.TEXT_FIELD);
        return tf.transform(Lists.newArrayList(text.split(" ")));
      }
    });


    JavaRDD<LabeledPoint> positiveExamples = spamFeatures.map(new Function<Vector, LabeledPoint>() {
      @Override
      public LabeledPoint call(Vector vector) throws Exception {
        return new LabeledPoint(1, vector);
      }
    });

    JavaRDD<LabeledPoint> negativeExamples = hamFeatures.map(new Function<Vector, LabeledPoint>() {
      @Override
      public LabeledPoint call(Vector vector) throws Exception {
        return new LabeledPoint(0, vector);
      }
    });

    JavaRDD<LabeledPoint> trainingData = positiveExamples.union(negativeExamples);
    trainingData.cache();

    final NaiveBayesModel model = NaiveBayes.train(trainingData.rdd(), 1.0);

    // save the model to a file in the output FileSet
    JavaSparkContext javaSparkContext = sparkContext.getOriginalSparkContext();
    FileSet outputFS = sparkContext.getDataset(OUTPUT_FILESET);
    model.save(JavaSparkContext.toSparkContext(javaSparkContext),
               outputFS.getBaseLocation().append("output").toURI().getPath());

    JavaPairRDD<LongWritable, Text> textsToClassify = sparkContext.readFromStream("textsToClassify", Text.class);
    JavaRDD<Vector> featuresToClassify = textsToClassify.map(new Function<Tuple2<LongWritable, Text>, Vector>() {
      @Override
      public Vector call(Tuple2<LongWritable, Text> longWritableTextTuple2) throws Exception {
        String text = longWritableTextTuple2._2().toString();
        return tf.transform(Lists.newArrayList(text.split(" ")));
      }
    });

    JavaRDD<Double> predict = model.predict(featuresToClassify);
    LOG.info("Predictions: {}", predict.collect());

    // key the predictions with the message
    JavaPairRDD<Text, Double> keyedPredictions = textsToClassify.values().zip(predict);

    // convert to byte[],byte[] to write to data
    JavaPairRDD<byte[], byte[]> bytesRDD =
      keyedPredictions.mapToPair(new PairFunction<Tuple2<Text, Double>, byte[], byte[]>() {
        @Override
        public Tuple2<byte[], byte[]> call(Tuple2<Text, Double> tuple) throws Exception {
          return new Tuple2<>(Bytes.toBytes(tuple._1().toString()), Bytes.toBytes(tuple._2()));
        }
      });

    sparkContext.writeToDataset(bytesRDD, CLASSIFIED_TEXTS, byte[].class, byte[].class);
  }
}
