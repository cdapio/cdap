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

package co.cask.cdap.examples.wikipedia

import co.cask.cdap.api.common.Bytes
import co.cask.cdap.api.dataset.table.Table
import co.cask.cdap.api.dataset.table.Put
import co.cask.cdap.api.spark.{SparkContext, ScalaSparkProgram}
import co.cask.cdap.api.workflow.Value
import co.cask.cdap.api.workflow.WorkflowToken
import org.apache.spark.{AccumulatorParam, Accumulator}
import org.apache.spark.mllib.clustering.{LDAModel, LDA}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.{RDD, NewHadoopRDD}

import java.text.BreakIterator
import java.util

import scala.collection.mutable

/**
 * Scala program to run Latent Dirichlet Allocation (LDA) on wikipedia data.
 * This is an adaptation of
 * https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/mllib/LDAExample.scala
 * for CDAP.
 */
class ScalaSparkLDA extends ScalaSparkProgram {

  override def run(context: SparkContext): Unit = {

    val arguments: util.Map[String, String] = context.getRuntimeArguments

    val normalizedWikiDataset: NewHadoopRDD[Array[Byte], Array[Byte]] =
      context.readFromDataset(WikipediaPipelineApp.NORMALIZED_WIKIPEDIA_DATASET, classOf[Array[Byte]], classOf[Array[Byte]])

    val originalContext: org.apache.spark.SparkContext = context.getOriginalSparkContext
      .asInstanceOf[org.apache.spark.SparkContext]

    // Pre-process data for LDA
    val (corpus, vocabArray, actualNumTokens) = preProcess(normalizedWikiDataset, arguments, originalContext)
    corpus.cache()

    // Run LDA
    val ldaModel = runLDA(corpus, arguments)

    val table: Table = context.getDataset(WikipediaPipelineApp.SPARK_LDA_OUTPUT_DATASET)
    // Print the topics, showing the top-weighted terms for each topic.
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)
    val topics = topicIndices.map { case (terms, termWeights) =>
      terms.zip(termWeights).map { case (term, weight) => (vocabArray(term.toInt), weight) }
    }

    val numRecords: Accumulator[Int] = originalContext.accumulator(0, "num.records")
    val initial: Term = new Term("", 0.0)
    val highestScore: Accumulator[Term] =
      originalContext.accumulator(initial, "highest.score")(HighestAccumulatorParam)
    topics.zipWithIndex.foreach { case (topic, i) =>
      val put: Put = new Put(Bytes.toBytes(i))
      topic.foreach { case (term, weight) =>
        put.add(term, weight)
        highestScore += new Term(term, weight)
      }
      table.put(put)
      numRecords += 1
    }

    val token: WorkflowToken = context.getWorkflowToken
    if (token != null) {
      token.put("num.records", Value.of(numRecords.value))
      token.put("highest.score.term", highestScore.value.name)
      // TODO: CDAP-3089 Accept double values in workflow token
      token.put("highest.score.value", String.valueOf(highestScore.value.weight))
    }
  }

  private def preProcess(normalizedWikiDataset: NewHadoopRDD[Array[Byte], Array[Byte]],
                         arguments: util.Map[String, String],
                         sc: org.apache.spark.SparkContext): (RDD[(Long, Vector)], Array[String], Long) = {
    val stopwordFile = if (arguments.containsKey("stopwords.file")) arguments.get("stopwords.file") else ""
    val vocabSize: Int = if (arguments.containsKey("vocab.size")) arguments.get("vocab.size").toInt else 1000

    val tokenizer = new SimpleTokenizer(sc, stopwordFile)

    val tokenized: RDD[(Long, IndexedSeq[String])] = normalizedWikiDataset.zipWithIndex().map {
      case (text, id) => id -> tokenizer.getWords(Bytes.toString(text._2))
    }
    tokenized.cache()

    val wordCounts: RDD[(String, Long)] = tokenized
      .flatMap { case (_, tokens) => tokens.map(_ -> 1L) }
      .reduceByKey(_+_)
    wordCounts.cache()

    val fullVocabSize = wordCounts.count()

    // Select vocab
    //  (vocab: Map[word -> id], total tokens after selecting vocab)
    val (vocab: Map[String, Int], selectedTokenCount: Long) = {
      val tmpSortedWC: Array[(String, Long)] = if (vocabSize == -1 || fullVocabSize <= vocabSize) {
        // Use all terms
        wordCounts.collect().sortBy(-_._2)
      } else {
        // Sort terms to select vocab
        wordCounts.sortBy(_._2, ascending = false).take(vocabSize)
      }
      (tmpSortedWC.map(_._1).zipWithIndex.toMap, tmpSortedWC.map(_._2).sum)
    }

    val documents: RDD[(Long, Vector)] = tokenized.map { case (id, tokens) =>
      // Filter tokens by vocabulary, and create word count vector representation of document.
      val wc = new mutable.HashMap[Int, Int]()
      tokens.foreach { term =>
        if (vocab.contains(term)) {
          val termIndex = vocab(term)
          wc(termIndex) = wc.getOrElse(termIndex, 0) + 1
        }
      }
      val indices = wc.keys.toArray.sorted
      val values = indices.map(i => wc(i).toDouble)

      val sb: Vector = Vectors.sparse(vocab.size, indices, values)
      (id, sb)
    }

    val vocabArray = new Array[String](vocab.size)
    vocab.foreach { case (term, i) => vocabArray(i) = term }

    (documents, vocabArray, selectedTokenCount)
  }

  private def runLDA(corpus: RDD[(Long, Vector)],
                     arguments: util.Map[String, String]): LDAModel = {
    val k: Int = if (arguments.containsKey("num.topics")) arguments.get("num.topics").toInt else 10
    val maxIterations: Int = if (arguments.containsKey("max.iterations")) arguments.get("max.iterations").toInt else 10
    val lda = new LDA()
    lda.setK(k)
      .setMaxIterations(maxIterations)
      .setDocConcentration(-1)
      .setTopicConcentration(-1)
      .setCheckpointInterval(10)
    lda.run(corpus)
  }

  private class SimpleTokenizer(sc: org.apache.spark.SparkContext, stopwordFile: String) extends Serializable {

    private val stopwords: Set[String] = if (stopwordFile.isEmpty) {
      Set.empty[String]
    } else {
      val stopwordText = sc.textFile(stopwordFile).collect()
      stopwordText.flatMap(_.stripMargin.split("\\s+")).toSet
    }

    // Matches sequences of Unicode letters
    private val allWordRegex = "^(\\p{L}*)$".r

    // Ignore words shorter than this length.
    private val minWordLength = 3

    def getWords(text: String): IndexedSeq[String] = {

      val words = new mutable.ArrayBuffer[String]()

      // Use Java BreakIterator to tokenize text into words.
      val wb = BreakIterator.getWordInstance
      wb.setText(text)

      // current,end index start,end of each word
      var current = wb.first()
      var end = wb.next()
      while (end != BreakIterator.DONE) {
        // Convert to lowercase
        val word: String = text.substring(current, end).toLowerCase
        // Remove short words and strings that aren't only letters
        word match {
          case allWordRegex(w) if w.length >= minWordLength && !stopwords.contains(w) =>
            words += w
          case _ =>
        }

        current = end
        try {
          end = wb.next()
        } catch {
          case e: Exception =>
            // Ignore remaining text in line.
            // This is a known bug in BreakIterator (for some Java versions),
            // which fails when it sees certain characters.
            end = BreakIterator.DONE
        }
      }
      words
    }
  }
}

class Term(termName: String, termWeight: Double) {
  val name: String = termName
  val weight: Double = termWeight
}

object HighestAccumulatorParam extends AccumulatorParam[Term] {
  def zero(initialValue: Term): Term = {
    initialValue
  }

  def addInPlace(v1: Term, v2: Term): Term = {
    if (v1.weight >= v2.weight) v1 else v2
  }
}
