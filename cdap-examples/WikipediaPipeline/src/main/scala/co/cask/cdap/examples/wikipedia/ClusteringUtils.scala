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

import java.text.BreakIterator

import co.cask.cdap.api.TxRunnable
import co.cask.cdap.api.common.Bytes
import co.cask.cdap.api.data.DatasetContext
import co.cask.cdap.api.dataset.table.{Put, Table}
import co.cask.cdap.api.spark.SparkExecutionContext
import co.cask.cdap.api.workflow.Value
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Accumulator, AccumulatorParam, SparkContext}

import scala.collection.mutable

/**
 * Utilities for Clustering algorithms used in {@link WikipediaPipelineApp}.
 */
object ClusteringUtils {

  /**
   * Pre-processes normalized wikipedia data to return a corpus (an RDD[(Long,Vector)], a vocabulary
   * and a count of tokens.
   */
  def preProcess(normalizedWikiDataset: RDD[(Array[Byte], Array[Byte])],
                 arguments: Map[String, String]): (RDD[(Long, Vector)], Array[String], Long) = {

    val stopwordFile = arguments.getOrElse("stopwords.file", "")
    val vocabSize = arguments.get("vocab.size").map(_.toInt).getOrElse(1000)

    val tokenizer = new SimpleTokenizer(normalizedWikiDataset.sparkContext, stopwordFile)

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

  /**
   * Store the clustering results in the specified dataset and update accumulators and workflow token.
   */
  def storeResults(sc: SparkContext, sec: SparkExecutionContext,
                   results: Array[Array[(String, Double)]], namespace: String, tableName: String) = {
    val numRecords: Accumulator[Int] = sc.accumulator(0, "num.records")
    val initial= new Term("", 0.0)
    val highestScore: Accumulator[Term] = sc.accumulator(initial, "highest.score")(HighestAccumulatorParam)

    sec.execute(new TxRunnable {
      override def run(context: DatasetContext) = {
        val table: Table = context.getDataset(namespace, tableName)
        results.zipWithIndex.foreach { case (topic, i) =>
          val put: Put = new Put(Bytes.toBytes(i))
          topic.foreach { case (term, weight) =>
            put.add(term, weight)
            highestScore += new Term(term, weight)
          }
          table.put(put)
          numRecords += 1
        }
      }
    })

    sec.getWorkflowToken.foreach(token => {
      token.put("num.records", Value.of(numRecords.value))
      token.put("highest.score.term", highestScore.value.name)
      token.put("highest.score.value", Value.of(highestScore.value.weight))
    })
  }
}

class SimpleTokenizer(sc: SparkContext, stopwordFile: String) extends Serializable {

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
