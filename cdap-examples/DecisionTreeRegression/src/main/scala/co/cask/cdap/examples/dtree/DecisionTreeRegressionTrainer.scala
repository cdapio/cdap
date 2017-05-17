/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.examples.dtree

import java.util.UUID
import java.util.concurrent.atomic.AtomicReference

import co.cask.cdap.api.TxRunnable
import co.cask.cdap.api.data.DatasetContext
import co.cask.cdap.api.dataset.lib.{FileSet, ObjectMappedTable}
import co.cask.cdap.api.spark.{SparkExecutionContext, SparkMain}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.{DecisionTreeRegressionModel, DecisionTreeRegressor}
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

/**
 * Implementation of Decision Tree Regression Spark Program.
 */
class DecisionTreeRegressionTrainer extends SparkMain {

  import DecisionTreeRegressionTrainer._

  override def run(implicit sec: SparkExecutionContext): Unit = {
    val inputPath = new AtomicReference[String]()
    val outputPath = new AtomicReference[String]()
    sec.execute(new TxRunnable {
      override def run(context: DatasetContext): Unit = {
        val inputDataset: FileSet = context.getDataset(DecisionTreeRegressionApp.TRAINING_DATASET)
        val outputDataset: FileSet = context.getDataset(DecisionTreeRegressionApp.MODEL_DATASET)
        inputPath.set(inputDataset.getBaseLocation.append("labels").toURI.getPath)
        outputPath.set(outputDataset.getBaseLocation.toURI.getPath)
      }
    })

    val spark = SparkSession
      .builder
      .appName("DecisionTreeRegressionExample")
      .getOrCreate()

    // Load the data stored in LIBSVM format as a DataFrame.
    val data = spark.read.format("libsvm").load(inputPath.get())

    // Automatically identify categorical features, and index them.
    // Here, we treat features with > 4 distinct values as continuous.
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(data)

    // Split the data into training and test sets (30% held out for testing).
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    // Train a DecisionTree model.
    val dt = new DecisionTreeRegressor()
      .setLabelCol("label")
      .setFeaturesCol("indexedFeatures")

    // Chain indexer and tree in a Pipeline.
    val pipeline = new Pipeline().setStages(Array(featureIndexer, dt))

    // Train model. This also runs the indexer.
    val model = pipeline.fit(trainingData)

    // Make predictions.
    val predictions = model.transform(testData)

    // Select example rows to display.
    val numCorrect = predictions.where("prediction=label").count()
    val numPredictions = predictions.count()

    // Select (prediction, true label) and compute test error.
    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)
    LOG.info("Root Mean Squared Error (RMSE) on test data = " + rmse)

    val treeModel = model.stages(1).asInstanceOf[DecisionTreeRegressionModel]
    LOG.info("Learned regression tree model:\n" + treeModel.toDebugString)

    // save the model
    val id = UUID.randomUUID().toString
    treeModel.save(outputPath.get() + "/" + id)
    val meta = new ModelMeta(featureIndexer.numFeatures, numPredictions, numCorrect, rmse, 0.7)
    sec.execute(new TxRunnable {
      override def run(context: DatasetContext): Unit = {
        val metaTable: ObjectMappedTable[ModelMeta] = context.getDataset(DecisionTreeRegressionApp.MODEL_META)
        metaTable.write(id, meta)
      }
    })

    spark.stop()
  }
}

object DecisionTreeRegressionTrainer {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[DecisionTreeRegressionTrainer])
}
